// Copyright IBM Corp. 2014, 2025
// SPDX-License-Identifier: MPL-2.0

package multierror

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestBoundedGroupZeroValueMatchesGroup(t *testing.T) {
	// Like Group, the zero value retains every error; anonymous errors are
	// never deduplicated.
	var g BoundedGroup
	err1 := errors.New("group_test: 1")
	err2 := errors.New("group_test: 2")
	g.Go(func() error { return err1 })
	g.Go(func() error { return err2 })
	g.Go(func() error { return err1 })

	err := g.Wait()
	if err == nil {
		t.Fatal("expected an error")
	}
	if got := len(err.Errors); got != 3 {
		t.Fatalf("expected 3 retained errors, got %d", got)
	}
	if !errors.Is(err, err1) || !errors.Is(err, err2) {
		t.Fatalf("errors.Is failed: %v", err)
	}
	if got := g.Omitted(); got != 0 {
		t.Fatalf("Omitted() = %d, want 0", got)
	}
}

func TestBoundedGroupEmpty(t *testing.T) {
	t.Run("zero value", func(t *testing.T) {
		var g BoundedGroup
		if err := g.Wait(); err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
		if got := g.Omitted(); got != 0 {
			t.Fatalf("expected 0 omitted, got %d", got)
		}
	})

	t.Run("nil tasks", func(t *testing.T) {
		g := NewBoundedGroup(10)
		g.Go(nil)
		g.GoKeyed("k", nil)
		if err := g.Wait(); err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
	})

	t.Run("nil tasks start no goroutines", func(t *testing.T) {
		before := runtime.NumGoroutine()
		g := NewBoundedGroup(10)
		g.Go(nil)
		g.GoKeyed("k", nil)
		_ = g.Wait()
		if after := runtime.NumGoroutine(); after > before {
			t.Fatalf("goroutines grew from %d to %d", before, after)
		}
	})

	t.Run("successful tasks", func(t *testing.T) {
		g := NewBoundedGroup(10)
		g.Go(func() error { return nil })
		g.GoKeyed("a", func() error { return nil })
		if err := g.Wait(); err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
	})
}

func TestBoundedGroupLimit(t *testing.T) {
	const limit = 5
	g := NewBoundedGroup(limit)
	for i := 0; i < 1000; i++ {
		i := i
		g.Go(func() error { return fmt.Errorf("failure %d", i) })
	}

	err := g.Wait()
	if err == nil {
		t.Fatal("expected an error")
	}
	if got := len(err.Errors); got != limit {
		t.Fatalf("expected %d retained errors, got %d", limit, got)
	}
	if got := g.Retained(); got != limit {
		t.Fatalf("Retained() = %d, want %d", got, limit)
	}
	if got, want := g.Omitted(), 1000-limit; got != want {
		t.Fatalf("Omitted() = %d, want %d", got, want)
	}

	// The summary must be visible to a caller that only has the string.
	if got := err.Error(); !strings.Contains(got, fmt.Sprintf("%d errors omitted", 1000-limit)) {
		t.Fatalf("error summary missing omitted count: %s", got)
	}
}

func TestBoundedGroupLimitDoesNotBlock(t *testing.T) {
	// Every task reports an error past the limit; Wait must still return and
	// every goroutine must exit.
	const limit = 3
	const tasks = 200

	g := NewBoundedGroup(limit)
	started := make(chan struct{}, tasks)
	for i := 0; i < tasks; i++ {
		i := i
		g.Go(func() error {
			started <- struct{}{}
			return fmt.Errorf("failure %d", i)
		})
	}

	done := make(chan *Error, 1)
	go func() { done <- g.Wait() }()

	for i := 0; i < tasks; i++ {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("tasks did not all start; collector appears to block workers")
		}
	}

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected an error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not return")
	}

	if got, want := g.Omitted(), tasks-limit; got != want {
		t.Fatalf("Omitted() = %d, want %d", got, want)
	}
}

func TestBoundedGroupDedupe(t *testing.T) {
	t.Run("first failure per key wins", func(t *testing.T) {
		g := NewBoundedGroup(10)
		for _, resource := range []string{"a", "b", "c"} {
			resource := resource
			for i := 0; i < 50; i++ {
				i := i
				g.GoKeyed(resource, func() error {
					return fmt.Errorf("%s: failure %d", resource, i)
				})
			}
		}

		err := g.Wait()
		if err == nil {
			t.Fatal("expected an error")
		}
		if got := len(err.Errors); got != 3 {
			t.Fatalf("expected 3 retained errors, got %d: %v", got, err)
		}
		if got := g.Omitted(); got != 0 {
			t.Fatalf("duplicates must not be counted as omitted, got %d", got)
		}
		for _, resource := range []string{"a", "b", "c"} {
			var matches int
			for _, e := range err.Errors {
				if strings.HasPrefix(e.Error(), resource+":") {
					matches++
				}
			}
			if matches != 1 {
				t.Fatalf("expected exactly one error for %q, got %d (%v)", resource, matches, err)
			}
		}
	})

	t.Run("duplicate keys do not consume capacity", func(t *testing.T) {
		g := NewBoundedGroup(2)
		for i := 0; i < 100; i++ {
			g.GoKeyed("same", func() error { return errors.New("same failure") })
		}
		if got := g.Retained(); got > 1 {
			t.Fatalf("expected at most one retained error, got %d", got)
		}
		if got := g.Omitted(); got != 0 {
			t.Fatalf("expected no omitted errors, got %d", got)
		}
	})

	t.Run("duplicates after the limit are not counted", func(t *testing.T) {
		// A source that keeps failing is counted at most once, even when its
		// first failure was itself omitted.
		g := NewBoundedGroup(1)
		g.AddKeyed("retained", errors.New("retained"))
		g.AddKeyed("other", errors.New("first"))
		g.AddKeyed("other", errors.New("second"))
		g.AddKeyed("other", errors.New("third"))
		if got := g.Omitted(); got != 1 {
			t.Fatalf("expected 1 omitted error, got %d", got)
		}
		if got := g.Retained(); got != 1 {
			t.Fatalf("expected 1 retained error, got %d", got)
		}
	})

	t.Run("retained errors stay bounded", func(t *testing.T) {
		g := NewBoundedGroup(3)
		for i := 0; i < 1000; i++ {
			i := i
			g.GoKeyed(fmt.Sprintf("resource-%d", i), func() error {
				return errors.New("failed")
			})
		}
		_ = g.Wait()
		if got := g.Retained(); got != 3 {
			t.Fatalf("Retained() = %d, want 3", got)
		}
		if got := g.Omitted(); got != 997 {
			t.Fatalf("Omitted() = %d, want 997", got)
		}
	})

	t.Run("anonymous errors are not deduplicated", func(t *testing.T) {
		g := NewBoundedGroup(10)
		for i := 0; i < 4; i++ {
			g.Go(func() error { return errors.New("same message") })
		}
		if got := len(g.Wait().Errors); got != 4 {
			t.Fatalf("expected 4 errors, got %d", got)
		}
	})

	t.Run("WithoutDedupe", func(t *testing.T) {
		g := NewBoundedGroup(10, WithoutDedupe())
		for i := 0; i < 4; i++ {
			g.GoKeyed("same", func() error { return errors.New("same failure") })
		}
		if got := len(g.Wait().Errors); got != 4 {
			t.Fatalf("expected 4 errors with dedupe disabled, got %d", got)
		}
	})

	t.Run("WithDedupe re-enables dedupe", func(t *testing.T) {
		g := NewBoundedGroup(10, WithoutDedupe(), WithDedupe(true))
		for i := 0; i < 4; i++ {
			g.GoKeyed("same", func() error { return errors.New("same failure") })
		}
		if got := len(g.Wait().Errors); got != 1 {
			t.Fatalf("expected 1 deduplicated error, got %d", got)
		}
	})
}

func TestBoundedGroupStableOrder(t *testing.T) {
	// Submit keys in one order but make them finish in another; the result
	// must follow the key order, not the completion order.
	keys := []string{"delta", "alpha", "charlie", "bravo"}
	release := make(map[string]chan struct{}, len(keys))
	for _, key := range keys {
		release[key] = make(chan struct{})
	}

	g := NewBoundedGroup(10)
	var wg sync.WaitGroup
	for _, key := range keys {
		key := key
		wg.Add(1)
		g.GoKeyed(key, func() error {
			defer wg.Done()
			<-release[key]
			return fmt.Errorf("%s failed", key)
		})
	}

	// Release in reverse order of submission.
	for i := len(keys) - 1; i >= 0; i-- {
		close(release[keys[i]])
	}
	wg.Wait()

	err := g.Wait()
	if err == nil {
		t.Fatal("expected an error")
	}
	if got, want := len(err.Errors), len(keys); got != want {
		t.Fatalf("expected %d errors, got %d", want, got)
	}
	want := []string{"alpha failed", "bravo failed", "charlie failed", "delta failed"}
	for i, e := range err.Errors {
		if e.Error() != want[i] {
			t.Fatalf("error %d = %q, want %q (all: %v)", i, e, want[i], err.Errors)
		}
	}

	// Repeated Wait calls return the same value and order.
	second := g.Wait()
	if second != err {
		t.Fatal("repeated Wait should return the same snapshot")
	}
}

func TestBoundedGroupErrorsIsAs(t *testing.T) {
	sentinel := errors.New("sentinel")
	var typed *typedError

	g := NewBoundedGroup(10)
	g.GoKeyed("a", func() error { return fmt.Errorf("wrapped: %w", sentinel) })
	g.GoKeyed("b", func() error { return &typedError{msg: "typed"} })
	g.GoKeyed("c", func() error { return nil })

	err := g.Wait()
	if err == nil {
		t.Fatal("expected an error")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("errors.Is could not find the sentinel: %v", err)
	}
	if !errors.As(err, &typed) {
		t.Fatalf("errors.As could not find the typed error: %v", err)
	}
	if typed.msg != "typed" {
		t.Fatalf("unexpected typed error: %#v", typed)
	}
}

type typedError struct{ msg string }

func (e *typedError) Error() string { return e.msg }

func TestBoundedGroupRepeatedWait(t *testing.T) {
	g := NewBoundedGroup(2)
	g.Go(func() error { return errors.New("first") })

	first := g.Wait()
	second := g.Wait()
	if first != second {
		t.Fatal("repeated Wait should return the same snapshot")
	}
	if got := g.Omitted(); got != 0 {
		t.Fatalf("Omitted() changed across Wait calls: %d", got)
	}

	// Adding work after a completed round starts a new snapshot without
	// mutating the value already returned.
	g.Go(func() error { return errors.New("second") })
	third := g.Wait()
	if len(first.Errors) != 1 {
		t.Fatalf("previous snapshot was modified: %v", first.Errors)
	}
	if len(third.Errors) != 2 {
		t.Fatalf("expected 2 errors in the new snapshot, got %v", third.Errors)
	}
}

func TestBoundedGroupConcurrentSubmitAndWait(t *testing.T) {
	const limit = 10
	const workers = 50
	const perWorker = 20
	const total = workers * perWorker

	g := NewBoundedGroup(limit)
	var wg sync.WaitGroup

	// Wait concurrently with submission; every call must return a coherent
	// snapshot and never count an omission twice.
	var waitWG sync.WaitGroup
	for i := 0; i < 4; i++ {
		waitWG.Add(1)
		go func() {
			defer waitWG.Done()
			if err := g.Wait(); err != nil {
				if got := len(err.Errors); got > limit {
					t.Errorf("snapshot retained %d errors, limit is %d", got, limit)
				}
			}
		}()
	}

	for w := 0; w < workers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				i := i
				g.GoKeyed(fmt.Sprintf("worker-%d-resource-%d", w, i), func() error {
					return fmt.Errorf("worker %d failure %d", w, i)
				})
			}
		}()
	}
	wg.Wait()
	waitWG.Wait()

	err := g.Wait()
	if err == nil {
		t.Fatal("expected an error")
	}
	if got := len(err.Errors); got != limit {
		t.Fatalf("expected %d retained errors, got %d", limit, got)
	}
	if got, want := g.Omitted(), total-limit; got != want {
		t.Fatalf("Omitted() = %d, want %d", got, want)
	}
	// A further Wait must not change the count.
	if got, want := g.Omitted(), total-limit; got != want {
		t.Fatalf("Omitted() changed after repeated Wait: %d, want %d", got, want)
	}
}

func TestBoundedGroupContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	g := NewBoundedGroup(10, WithContext(ctx))

	started := make(chan struct{})
	release := make(chan struct{})
	g.GoKeyed("blocked", func() error {
		close(started)
		<-release
		return errors.New("late failure")
	})

	<-started

	waited := make(chan *Error, 1)
	go func() { waited <- g.Wait() }()

	cancel()

	select {
	case err := <-waited:
		if err == nil {
			t.Fatal("expected the cancellation to be reported")
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("errors.Is(err, context.Canceled) = false: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Wait was not woken by cancellation")
	}

	// New errors are no longer accepted.
	g.AddKeyed("after-cancel", errors.New("ignored"))
	if got := g.Omitted(); got != 0 {
		t.Fatalf("errors after cancellation must not be counted, got %d omitted", got)
	}

	// The started task is still reclaimed when it finishes.
	close(release)
	if err := g.Wait(); err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation to be retained, got %v", err)
	}
}

func TestBoundedGroupContextCancelAfterCompletionIsNotRewritten(t *testing.T) { // Cancelling a scope after its work finished must not turn a stable
	// result into a cancellation error on a later Wait.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	g := NewBoundedGroup(10, WithContext(ctx))
	g.GoKeyed("a", func() error { return errors.New("failure") })

	first := g.Wait()
	if first == nil || errors.Is(first, context.Canceled) {
		t.Fatalf("unexpected result before cancellation: %v", first)
	}

	cancel()
	second := g.Wait()
	if second != first {
		t.Fatalf("cancellation rewrote a completed result: %v -> %v", first, second)
	}
}

func TestBoundedGroupContextCancelBeforeWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	g := NewBoundedGroup(10, WithContext(ctx))
	if err := g.Wait(); err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestBoundedGroupContextDeadlineExceeded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	<-ctx.Done()

	g := NewBoundedGroup(10, WithContext(ctx))
	if err := g.Wait(); err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
}

func TestBoundedGroupContextCancelDoesNotBlockTasks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	g := NewBoundedGroup(1, WithContext(ctx))

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		i := i
		wg.Add(1)
		g.Go(func() error {
			defer wg.Done()
			return fmt.Errorf("failure %d", i)
		})
	}

	cancel()
	exited := make(chan struct{})
	go func() { wg.Wait(); close(exited) }()
	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		t.Fatal("tasks did not finish after cancellation")
	}
	_ = g.Wait()
}

func TestBoundedGroupNoBackgroundGoroutines(t *testing.T) {
	before := runtime.NumGoroutine()

	g := NewBoundedGroup(2, WithContext(context.Background()))
	if err := g.Wait(); err != nil {
		t.Fatalf("empty group Wait: %v", err)
	}
	g.Go(func() error { return nil })
	g.Go(func() error { return nil })
	_ = g.Wait()

	// Allow runtime bookkeeping to settle.
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		runtime.Gosched()
		time.Sleep(time.Millisecond)
	}
	if after := runtime.NumGoroutine(); after > before {
		t.Fatalf("goroutines leaked: before %d, after %d", before, after)
	}
}

func TestBoundedGroupZeroLimit(t *testing.T) {
	// A limit of zero means unlimited, matching the zero value.
	g := NewBoundedGroup(0)
	for i := 0; i < 10; i++ {
		i := i
		g.GoKeyed(fmt.Sprintf("key-%d", i), func() error {
			return fmt.Errorf("failure %d", i)
		})
	}

	err := g.Wait()
	if err == nil {
		t.Fatal("expected an error")
	}
	if got := len(err.Errors); got != 10 {
		t.Fatalf("expected 10 retained errors, got %d", got)
	}
	if got := g.Omitted(); got != 0 {
		t.Fatalf("Omitted() = %d, want 0", got)
	}
	if got := g.Wait(); got != err {
		t.Fatal("repeated Wait should return the same snapshot")
	}
}

func TestBoundedGroupNegativeLimit(t *testing.T) {
	g := NewBoundedGroup(-5)
	if got := g.Limit(); got != 0 {
		t.Fatalf("Limit() = %d, want 0 (unlimited)", got)
	}
	g.AddKeyed("a", errors.New("first"))
	g.AddKeyed("b", errors.New("second"))
	if got := len(g.Wait().Errors); got != 2 {
		t.Fatalf("expected 2 retained errors, got %d", got)
	}
}

func TestBoundedGroupSingularSummary(t *testing.T) {
	g := NewBoundedGroup(1)
	g.AddKeyed("a", errors.New("retained"))
	g.AddKeyed("b", errors.New("omitted"))

	err := g.Wait()
	if got := err.Error(); !strings.Contains(got, "1 error omitted because the error limit was reached") {
		t.Fatalf("missing singular summary: %q", got)
	}
}

func TestBoundedGroupDefaultFormatUnchanged(t *testing.T) {
	g := NewBoundedGroup(5)
	g.AddKeyed("a", errors.New("foo"))
	g.AddKeyed("b", errors.New("bar"))

	want := "2 errors occurred:\n\t* foo\n\t* bar\n\n"
	if got := g.Wait().Error(); got != want {
		t.Fatalf("format changed without omissions:\n got: %q\nwant: %q", got, want)
	}
}

func TestBoundedGroupAdd(t *testing.T) {
	// The zero value retains errors like Group, and deduplicates keys.
	var g BoundedGroup
	g.AddKeyed("a", errors.New("first"))
	g.AddKeyed("a", errors.New("duplicate"))
	g.AddKeyed("", errors.New("anonymous"))
	g.AddKeyed("", errors.New("second anonymous"))
	g.AddKeyed("", nil)

	err := g.Wait()
	if err == nil {
		t.Fatal("expected an error")
	}
	if got := len(err.Errors); got != 3 {
		t.Fatalf("expected 3 retained errors, got %d: %v", got, err)
	}
	if got := g.Omitted(); got != 0 {
		t.Fatalf("Omitted() = %d, want 0", got)
	}

	bounded := NewBoundedGroup(2)
	bounded.AddKeyed("a", errors.New("first"))
	bounded.AddKeyed("a", errors.New("duplicate"))
	bounded.AddKeyed("b", errors.New("second"))
	bounded.AddKeyed("c", errors.New("third"))
	if got := len(bounded.Wait().Errors); got != 2 {
		t.Fatalf("expected 2 retained errors, got %d", got)
	}
	if got := bounded.Omitted(); got != 1 {
		t.Fatalf("Omitted() = %d, want 1", got)
	}
}

func TestBoundedGroupSetLimit(t *testing.T) {
	var g BoundedGroup
	g.AddKeyed("a", errors.New("first"))
	g.AddKeyed("b", errors.New("second"))
	if got := len(g.Wait().Errors); got != 2 {
		t.Fatalf("expected 2 errors before a limit is set, got %d", got)
	}

	g.SetLimit(2)
	g.AddKeyed("c", errors.New("third"))

	err := g.Wait()
	if got := len(err.Errors); got != 2 {
		t.Fatalf("expected 2 errors after the limit is set, got %d", got)
	}
	if got := g.Omitted(); got != 1 {
		t.Fatalf("Omitted() = %d, want 1", got)
	}

	// Removing the limit resumes retention.
	g.SetLimit(0)
	g.AddKeyed("f", errors.New("sixth"))
	if got := len(g.Wait().Errors); got != 3 {
		t.Fatalf("expected 3 errors after clearing the limit, got %d", got)
	}
}

func TestBoundedGroupRace(t *testing.T) {
	g := NewBoundedGroup(8)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			g.GoKeyed(fmt.Sprintf("key-%d", i%16), func() error {
				return fmt.Errorf("failure %d", i)
			})
		}()
	}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = g.Wait()
		}()
	}
	wg.Wait()

	err := g.Wait()
	if err == nil {
		t.Fatal("expected an error")
	}
	if got := len(err.Errors); got > 8 {
		t.Fatalf("retained %d errors, limit is 8", got)
	}
}
