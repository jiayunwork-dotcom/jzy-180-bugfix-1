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
