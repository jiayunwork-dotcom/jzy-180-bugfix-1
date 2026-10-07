package store_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"qcinspect/internal/store"
)

// TestConcurrentInsertSameStream hammers one stream from many goroutines with
// both in-order and backdated inserts. It then proves:
//   - no batch was lost (count and unique names match);
//   - the persisted state and every record equal a single-threaded full
//     replay (no double scoring / interleaved states);
//   - all writers serialized without deadlock.
func TestConcurrentInsertSameStream(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	nID, tID, rID := mkTriplePlans(t, st, "conc")
	sr, err := st.CreateStream(ctx, store.CreateStreamParams{
		Name: "conc-stream", NormalPlanID: nID, TightenedPlanID: tID, ReducedPlanID: rID,
	})
	if err != nil {
		t.Fatal(err)
	}
	sid := sr.ID

	const writers = 16
	const perWriter = 60
	var wg sync.WaitGroup
	var inserted, rejected int64
	start := make(chan struct{})
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			for i := 0; i < perWriter; i++ {
				// Mostly forward-time, occasionally backdated.
				hour := 1000 + w*1000 + i
				if i%7 == 0 {
					hour = 900 + ((w*13 + i) % 200)
				}
				_, err := st.AddBatch(ctx, sid, store.BatchInput{
					BatchNo:     fmt.Sprintf("W%02d-%04d", w, i),
					InspectedAt: at(hour),
					D1:          (w + i) % 9,
				})
				if err == nil {
					atomic.AddInt64(&inserted, 1)
				} else if err == store.ErrSuspended {
					atomic.AddInt64(&rejected, 1)
				} else {
					t.Errorf("unexpected insert error: %v", err)
					return
				}
			}
		}(w)
	}
	close(start)
	wg.Wait()

	rows, err := st.ListBatches(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(rows)) != inserted {
		t.Fatalf("lost or duplicated batches: list=%d committed inserts=%d rejected=%d",
			len(rows), inserted, rejected)
	}
	// Unique batch numbers: no two writes overwrote each other.
	seen := map[string]bool{}
	for _, r := range rows {
		if seen[r.BatchNo] {
			t.Fatalf("duplicate batch_no %s", r.BatchNo)
		}
		seen[r.BatchNo] = true
	}
	// The decisive check: state and records equal a fresh full replay.
	verifyAgainstReference(ctx, t, st, sid)
}

// TestConcurrentFlagTogglesAndBatches drives one stream with concurrent batch
// writers (in-order and backdated) interleaved with concurrent PATCH flag
// flips. After the storm it proves:
//   - no batch lost / duplicated;
//   - the stored state and every record equal a single-threaded full replay
//     of all batches, resumes and flags events (no double scoring, no
//     boundary shifting);
//   - every earlier batch is covered by at least one flag event after it,
//     yet none of their records depends on flag value in a way the replay
//     cannot reconstruct (that is exactly what verifyAgainstReference checks).
func TestConcurrentFlagTogglesAndBatches(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	nID, tID, rID := mkTriplePlans(t, st, "flagconc")
	sr, err := st.CreateStream(ctx, store.CreateStreamParams{
		Name: "flag-conc-stream", NormalPlanID: nID, TightenedPlanID: tID, ReducedPlanID: rID,
	})
	if err != nil {
		t.Fatal(err)
	}
	sid := sr.ID

	const writers = 12
	const perWriter = 50
	const flagWriters = 4
	const perFlagWriter = 60
	var wg sync.WaitGroup
	var inserted, rejected, flagOK int64
	start := make(chan struct{})
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			for i := 0; i < perWriter; i++ {
				hour := 2000 + w*1000 + i
				if i%5 == 0 {
					hour = 1000 + ((w*29 + i) % 400)
				}
				_, err := st.AddBatch(ctx, sid, store.BatchInput{
					BatchNo:     fmt.Sprintf("W%02d-%04d", w, i),
					InspectedAt: at(hour),
					D1:          (w*7 + i) % 9,
				})
				if err == nil {
					atomic.AddInt64(&inserted, 1)
				} else if err == store.ErrSuspended {
					atomic.AddInt64(&rejected, 1)
				} else {
					t.Errorf("unexpected insert error: %v", err)
					return
				}
			}
		}(w)
	}
	v := true
	f := false
	for w := 0; w < flagWriters; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			for i := 0; i < perFlagWriter; i++ {
				// Mostly real changes, occasionally no-ops.
				ps, pa := &v, &f
				if (w+i)%2 == 0 {
					ps, pa = &f, &v
				}
				if i%13 == 0 {
					pa = nil // partial PATCH
				}
				if _, err := st.SetFlags(ctx, sid, ps, pa); err != nil {
					t.Errorf("unexpected flags error: %v", err)
					return
				}
				atomic.AddInt64(&flagOK, 1)
			}
		}(w)
	}
	close(start)
	wg.Wait()

	rows, err := st.ListBatches(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(rows)) != inserted {
		t.Fatalf("lost/duplicated batches: list=%d inserts=%d rejected=%d flags=%d",
			len(rows), inserted, rejected, flagOK)
	}
	seen := map[string]bool{}
	for _, r := range rows {
		if seen[r.BatchNo] {
			t.Fatalf("duplicate batch_no %s", r.BatchNo)
		}
		seen[r.BatchNo] = true
	}
	verifyAgainstReference(ctx, t, st, sid)
}

// TestConcurrentDifferentStreamsDoNotBlock proves streams are independent:
// writers on one stream never wait on locks held by another stream.
func TestConcurrentDifferentStreamsDoNotBlock(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	nID, tID, rID := mkTriplePlans(t, st, "multi")
	const streams = 8
	var sids []int64
	for i := 0; i < streams; i++ {
		sr, err := st.CreateStream(ctx, store.CreateStreamParams{
			Name:            fmt.Sprintf("s-%d", i),
			NormalPlanID:    nID,
			TightenedPlanID: tID,
			ReducedPlanID:   rID,
		})
		if err != nil {
			t.Fatal(err)
		}
		sids = append(sids, sr.ID)
	}
	var wg sync.WaitGroup
	for i, sid := range sids {
		wg.Add(1)
		go func(i int, sid int64) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				if _, err := st.AddBatch(ctx, sid, store.BatchInput{
					BatchNo:     fmt.Sprintf("s%d-%d", i, j),
					InspectedAt: at(2000 + i*100 + j),
					D1:          j % 4,
				}); err != nil && err != store.ErrSuspended {
					t.Errorf("stream %d: %v", sid, err)
					return
				}
			}
		}(i, sid)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("parallel streams appear to block each other (timeout)")
	}
	for _, sid := range sids {
		verifyAgainstReference(ctx, t, st, sid)
	}
}
