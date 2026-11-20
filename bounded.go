// Copyright IBM Corp. 2014, 2025
// SPDX-License-Identifier: MPL-2.0

package multierror

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
)

// BoundedGroupOption configures a BoundedGroup. Options are applied once by
// NewBoundedGroup, before the group is used.
type BoundedGroupOption func(*BoundedGroup)

// WithContext makes a BoundedGroup observe ctx.
//
// Once ctx is cancelled the group stops accepting new errors: failures
// recorded after that point are dropped instead of being counted. A Wait call
// blocked on running tasks is woken immediately and returns the errors
// retained so far, plus the context error so that errors.Is(err,
// context.Canceled) or errors.Is(err, context.DeadlineExceeded) succeeds.
//
// Tasks that were already running are not abandoned. They run to completion
// and their goroutines exit normally, but their late errors are ignored. A nil
// ctx is treated the same as context.Background().
func WithContext(ctx context.Context) BoundedGroupOption {
	return func(g *BoundedGroup) {
		g.ctx = ctx
	}
}

// WithDedupe enables or disables deduplication by source key. Deduplication is
// enabled by default, including for the zero value.
func WithDedupe(enable bool) BoundedGroupOption {
	return func(g *BoundedGroup) {
		g.noDedupe = !enable
	}
}

// WithoutDedupe disables deduplication by source key, so every recorded
// failure takes its own slot until the retention limit is reached.
func WithoutDedupe() BoundedGroupOption {
	return WithDedupe(false)
}

// BoundedGroup is a concurrency-safe error collector with bounded memory use.
//
// Like Group, it runs functions in goroutines and coalesces the errors they
// return. Unlike Group, it retains at most the number of errors it was created
// with. Once that capacity is exhausted it keeps running tasks, but only
// increments a count of omitted failures, so a failing task never blocks on a
// full collector and memory use stays bounded no matter how many failures
// occur.
//
// Failures submitted with GoKeyed (or AddKeyed with a non-empty key) carry a
// source key. With deduplication enabled, only the first failure recorded for a
// key is kept; later failures from the same source are discarded and are not
// counted as omitted, whether the first failure was retained or dropped by the
// limit. The key set therefore follows the number of failing sources, while the
// stored errors are bounded by the limit. Errors submitted without a key are
// never deduplicated against each other.
//
// Wait returns the retained errors in a deterministic order derived from the
// source key and the error message, independent of the order in which tasks
// happened to finish. When failures had to be dropped by the retention limit,
// the formatted result ends with a summary line giving their count, and
// Omitted reports the same number. The returned *Error still supports
// errors.Is and errors.As for every retained cause.
//
// A BoundedGroup never starts background goroutines of its own. Goroutines are
// created only by Go and GoKeyed, one per submitted function, and each exits
// when its function returns.
//
// A BoundedGroup is safe for concurrent use.
//
// The zero value is ready to use and, like Group, retains every error; there is
// no limit until one is set with NewBoundedGroup. Source-key deduplication is
// still enabled by default.
type BoundedGroup struct {
	ctx      context.Context
	limit    int
	noDedupe bool

	mu sync.Mutex

	// errs holds at most limit entries when a limit is set; entries are
	// copied, never mutated in place once stored. seen holds one short key per
	// distinct failing source so the same resource is not reported or counted
	// repeatedly.
	errs    []boundedEntry
	seen    map[string]struct{}
	omitted int
	seq     uint64

	// active counts submitted functions that have not returned yet. wait is
	// non-nil exactly while active > 0, and is closed when the last function
	// returns so blocked Wait calls can wake without a background goroutine.
	active    int
	wait      chan struct{}
	cancelled bool

	// snapshot caches the result of the current round. It is invalidated
	// whenever new state is recorded, which makes repeated Wait calls cheap
	// and keeps them from re-counting omissions.
	snapshot      *Error
	snapshotReady bool
}

// boundedEntry is one retained error and its source key.
type boundedEntry struct {
	key string
	err error
	seq uint64
}

// NewBoundedGroup creates a BoundedGroup that retains at most limit errors.
//
// If more than limit distinct failures are reported, the extras are counted by
// Omitted instead of being stored. A limit less than or equal to zero means
// unlimited, matching the convention of Group and errgroup.SetLimit: the group
// then retains every distinct failure, like the zero value.
//
// By default, keyed errors are deduplicated by source key. Use WithDedupe or
// WithoutDedupe to change that, and WithContext to make the group observe a
// context.
func NewBoundedGroup(limit int, opts ...BoundedGroupOption) *BoundedGroup {
	if limit < 0 {
		limit = 0
	}
	g := &BoundedGroup{limit: limit}
	for _, opt := range opts {
		if opt != nil {
			opt(g)
		}
	}
	return g
}

// Go calls f in a new goroutine. The error returned by f is attributed to an
// anonymous source and is never deduplicated against another error. Go is
// equivalent to GoKeyed("", f).
//
// A nil f is ignored.
func (g *BoundedGroup) Go(f func() error) {
	g.GoKeyed("", f)
}

// GoKeyed calls f in a new goroutine and attributes the error it returns to
// key. With deduplication enabled (the default), only the first failure
// recorded for a key is kept; later failures from the same key are discarded
// without being counted as omitted. An empty key is equivalent to calling Go.
//
// A nil f is ignored. Work may be submitted after Wait returned; the next Wait
// returns an updated snapshot, while results already handed to earlier callers
// are never modified.
func (g *BoundedGroup) GoKeyed(key string, f func() error) {
	if f == nil {
		return
	}

	g.mu.Lock()
	g.active++
	if g.wait == nil {
		g.wait = make(chan struct{})
	}
	g.mu.Unlock()

	go func() {
		err := f()

		g.mu.Lock()
		g.recordLocked(key, err)
		g.active--
		if g.active == 0 && g.wait != nil {
			close(g.wait)
			g.wait = nil
		}
		g.mu.Unlock()
	}()
}

// Add records err from an anonymous source without starting a goroutine. It is
// equivalent to AddKeyed("", err) and never deduplicates.
//
// Add never blocks on the retention limit: once the limit is reached the error
// is counted by Omitted instead.
//
// Add is safe to call concurrently with Go, GoKeyed, AddKeyed and Wait.
func (g *BoundedGroup) Add(err error) {
	g.AddKeyed("", err)
}

// AddKeyed records err for the given source key without starting a goroutine.
//
// The same capacity and deduplication rules apply as for GoKeyed: an empty key
// means an anonymous source, and with deduplication enabled only the first
// failure recorded for a key is kept.
//
// AddKeyed is safe to call concurrently with Go, GoKeyed, Add and Wait.
func (g *BoundedGroup) AddKeyed(key string, err error) {
	g.mu.Lock()
	g.recordLocked(key, err)
	g.mu.Unlock()
}

// recordLocked stores err if cancellation, capacity and deduplication allow
// it. The caller must hold g.mu. The critical section is O(1) and never waits
// for another worker, so reaching the limit cannot block a task goroutine.
func (g *BoundedGroup) recordLocked(key string, err error) {
	// Observe cancellation before looking at err: a task finishing after the
	// context was cancelled still latches the cancellation, even if it
	// succeeded, so the next Wait reports why collection stopped.
	if g.observeContextLocked() || err == nil {
		return
	}

	keyed := key != "" && !g.noDedupe
	if keyed {
		if _, ok := g.seen[key]; ok {
			// This source already failed once; only the first failure is
			// relevant, so the duplicate is dropped without being counted.
			return
		}
		if g.seen == nil {
			g.seen = make(map[string]struct{})
		}
		// Claim the key before the capacity check: whether the failure ends
		// up retained or omitted, later failures from this source must be
		// ignored.
		g.seen[key] = struct{}{}
	}

	if g.limitReachedLocked() {
		// Past the limit only the counter grows; the error itself is
		// discarded without blocking the worker.
		g.omitted++
		g.snapshotReady = false
		return
	}

	g.seq++
	g.errs = append(g.errs, boundedEntry{key: key, err: err, seq: g.seq})
	g.snapshotReady = false
}

// limitReachedLocked reports whether the retention limit, if any, is used up.
// The caller must hold g.mu. A limit of zero or less means unlimited.
func (g *BoundedGroup) limitReachedLocked() bool {
	return g.limit > 0 && len(g.errs) >= g.limit
}

// observeContextLocked reports whether the group is cancelled, latching the
// cancellation in that case. The caller must hold g.mu.
func (g *BoundedGroup) observeContextLocked() bool {
	if g.cancelled {
		return true
	}
	if g.ctx == nil || g.ctx.Err() == nil {
		return false
	}
	g.cancelled = true
	g.snapshotReady = false
	return true
}

// observeWaitCancelLocked reports whether Wait should stop and report the
// context error. It differs from observeContextLocked in one case: when a round
// has already produced its result and nothing is running, a later cancellation
// must not rewrite that result, so repeated Wait calls keep returning the same
// snapshot. The caller must hold g.mu.
func (g *BoundedGroup) observeWaitCancelLocked() bool {
	if g.cancelled {
		return true
	}
	if g.ctx == nil || g.ctx.Err() == nil {
		return false
	}
	if g.active == 0 && g.snapshotReady {
		return false
	}
	return g.observeContextLocked()
}

// Wait blocks until every function submitted so far has returned, or until the
// group's context is cancelled, and then returns the collected errors.
//
// Wait is safe to call more than once and from more than one goroutine. If no
// new failure has been recorded since a result was produced, repeated calls
// return the same snapshot without re-counting omissions. Failures recorded
// after a result was returned start a new snapshot; values already returned to
// other callers are never modified.
//
// If the context is cancelled, Wait stops waiting and returns immediately with
// the errors retained so far plus the context error, so that
// errors.Is(err, context.Canceled) or errors.Is(err, context.DeadlineExceeded)
// succeeds. Either way the cancellation is latched: errors returned by tasks
// after that point are ignored, but the tasks still run to completion and their
// goroutines exit normally.
//
// Wait returns nil when no errors were retained, nothing was omitted and the
// group was not cancelled. In every other case the returned *Error lists the
// retained causes in a stable order; if failures were dropped by the retention
// limit, its formatted message ends with a summary of their count.
func (g *BoundedGroup) Wait() *Error {
	g.mu.Lock()
	for {
		// Cancellation is reported once it is observed by Wait or by a
		// finishing task, and it stays latched so the result is stable. The
		// exception is a round that already produced a result and has no work
		// in flight: a later cancellation must not rewrite that value.
		if g.observeWaitCancelLocked() {
			res := g.resultLocked()
			g.mu.Unlock()
			return res
		}
		if g.active == 0 {
			res := g.resultLocked()
			g.mu.Unlock()
			return res
		}

		wait := g.wait
		var done <-chan struct{}
		if g.ctx != nil {
			done = g.ctx.Done()
		}
		g.mu.Unlock()

		if done == nil {
			<-wait
		} else {
			select {
			case <-wait:
			case <-done:
			}
		}

		g.mu.Lock()
	}
}

// resultLocked returns the cached result of the current round, building it on
// first use. The caller must hold g.mu.
func (g *BoundedGroup) resultLocked() *Error {
	if g.snapshotReady {
		return g.snapshot
	}
	g.snapshot = g.buildResultLocked()
	g.snapshotReady = true
	return g.snapshot
}

// buildResultLocked builds a new *Error snapshot. The caller must hold g.mu.
func (g *BoundedGroup) buildResultLocked() *Error {
	if len(g.errs) == 0 && g.omitted == 0 && !g.cancelled {
		return nil
	}

	entries := make([]boundedEntry, len(g.errs))
	copy(entries, g.errs)
	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].key != entries[j].key {
			return entries[i].key < entries[j].key
		}
		left, right := entries[i].err.Error(), entries[j].err.Error()
		if left != right {
			return left < right
		}
		return entries[i].seq < entries[j].seq
	})

	errs := make([]error, 0, len(entries)+1)
	for _, entry := range entries {
		errs = append(errs, entry.err)
	}

	// The cancellation cause is included even when the retention limit has
	// already been reached: it explains why Wait returned early and lets
	// callers match it with errors.Is.
	if g.cancelled && g.ctx != nil {
		if ctxErr := g.ctx.Err(); ctxErr != nil && !containsError(errs, ctxErr) {
			errs = append(errs, ctxErr)
		}
	}

	return &Error{
		Errors:      errs,
		ErrorFormat: boundedFormat(g.omitted),
	}
}

// containsError reports whether errs already contains target according to
// errors.Is.
func containsError(errs []error, target error) bool {
	for _, err := range errs {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

// boundedFormat returns a formatter that lists the retained errors and then
// summarizes the failures dropped by the retention limit. When nothing was
// omitted it is exactly ListFormatFunc.
func boundedFormat(omitted int) ErrorFormatFunc {
	return func(es []error) string {
		if len(es) == 0 {
			// Only reachable when every failure was omitted: there is no
			// retained cause to list, so the summary is the whole message.
			if omitted > 0 {
				return omissionSummary(omitted)
			}
			return ""
		}

		msg := ListFormatFunc(es)
		if omitted > 0 {
			msg += omissionSummary(omitted)
		}
		return msg
	}
}

// omissionSummary renders the truncation note appended to a bounded result.
func omissionSummary(omitted int) string {
	if omitted == 1 {
		return "1 error omitted because the error limit was reached\n"
	}
	return fmt.Sprintf("%d errors omitted because the error limit was reached\n", omitted)
}

// Limit returns the configured maximum number of retained errors. Zero means
// that no limit is configured: the group then retains every distinct failure,
// like the zero value.
func (g *BoundedGroup) Limit() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.limit
}

// SetLimit changes the number of errors the group retains. A limit less than or
// equal to zero means unlimited.
//
// Lowering the limit does not discard errors that are already retained; it
// only stops further errors from being stored, so the group may briefly hold
// more than the new limit. SetLimit is safe to call concurrently with
// submission and Wait.
func (g *BoundedGroup) SetLimit(limit int) {
	if limit < 0 {
		limit = 0
	}
	g.mu.Lock()
	g.limit = limit
	g.snapshotReady = false
	g.mu.Unlock()
}

// Retained returns the number of errors currently retained.
func (g *BoundedGroup) Retained() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.errs)
}

// Omitted returns the number of failures that were counted but not retained
// because the group had reached its limit. Failures discarded as duplicates of
// a retained source key are not counted, nor are failures recorded after the
// context was cancelled.
func (g *BoundedGroup) Omitted() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.omitted
}
