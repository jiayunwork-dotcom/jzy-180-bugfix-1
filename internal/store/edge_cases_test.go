package store_test

import (
	"context"
	"testing"

	"qcinspect/internal/statemachine"
	"qcinspect/internal/store"
)

// Toggle flags while suspended (after 5 cumulative tightened rejects): the
// flags event is written and folded but must not resume the stream.
func TestFlagToggleWhileSuspended(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	nID, tID, rID := mkTriplePlans(t, st, "susfl")
	sr, _ := st.CreateStream(ctx, store.CreateStreamParams{
		Name: "sus-flags", NormalPlanID: nID, TightenedPlanID: tID, ReducedPlanID: rID})
	// normal c=3: two rejects -> tightened
	st.AddBatch(ctx, sr.ID, store.BatchInput{BatchNo: "a", InspectedAt: at(1), D1: 4})
	st.AddBatch(ctx, sr.ID, store.BatchInput{BatchNo: "b", InspectedAt: at(2), D1: 4})
	// tightened c=1: 5 rejects -> suspended
	for i := 0; i < 9; i++ {
		mr, err := st.AddBatch(ctx, sr.ID, store.BatchInput{
			BatchNo: lotName(500 + i), InspectedAt: at(10 + i), D1: 2})
		if err != nil {
			t.Fatal(err)
		}
		if mr.State.Severity == statemachine.Suspended {
			break
		}
	}
	d, _ := st.GetStreamDetail(ctx, sr.ID)
	if d.State.(statemachine.State).Severity != statemachine.Suspended {
		t.Fatal("should be suspended")
	}
	on := true
	mr, err := st.SetFlags(ctx, sr.ID, &on, &on)
	if err != nil {
		t.Fatal(err)
	}
	if mr.State.Severity != statemachine.Suspended {
		t.Fatalf("flags must not resume suspension: %s", mr.State.Severity)
	}
	// Still cannot add a lot without resume.
	if _, err := st.AddBatch(ctx, sr.ID, store.BatchInput{
		BatchNo: "late", InspectedAt: at(500), D1: 0}); err != store.ErrSuspended {
		t.Fatalf("want ErrSuspended got %v", err)
	}
	// Resume after the flags boundary: starts tightened.
	st.Resume(ctx, sr.ID, at(600))
	mr, err = st.AddBatch(ctx, sr.ID, store.BatchInput{
		BatchNo: "after", InspectedAt: at(601), D1: 0})
	if err != nil {
		t.Fatal(err)
	}
	if mr.State.Severity != statemachine.Tightened {
		t.Fatalf("post-resume severity=%s want tightened", mr.State.Severity)
	}
	verifyAgainstReference(ctx, t, st, sr.ID)
}

// Delete all batches after flags events: subsequent PATCH must rewrite
// initial flags (from-start), and records stay replay-consistent.
func TestFlagAfterAllBatchesDeleted(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	nID, tID, rID := mkBoundaryPlans(t, st)
	sr, _ := st.CreateStream(ctx, store.CreateStreamParams{
		Name: "delall", NormalPlanID: nID, TightenedPlanID: tID, ReducedPlanID: rID})
	var ids []int64
	for i := 1; i <= 3; i++ {
		mr, err := st.AddBatch(ctx, sr.ID, store.BatchInput{
			BatchNo: "D" + itoa2(i), InspectedAt: atDay(i, 0), D1: 0})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, newestID(mr))
	}
	on := true
	if _, err := st.SetFlags(ctx, sr.ID, &on, &on); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		if _, err := st.DeleteBatch(ctx, sr.ID, id); err != nil {
			t.Fatal(err)
		}
	}
	// Stream empty again. PATCH off -> from start semantics.
	off := false
	if _, err := st.SetFlags(ctx, sr.ID, &off, &off); err != nil {
		t.Fatal(err)
	}
	d, err := st.GetStreamDetail(ctx, sr.ID)
	if err != nil {
		t.Fatal(err)
	}
	if d.ProductionStable || d.SupervisorApproval {
		t.Fatal("current flags should be false")
	}
	// New batches fold from start with flags off: 12 perfect lots stay normal.
	for i := 1; i <= 12; i++ {
		if _, err := st.AddBatch(ctx, sr.ID, store.BatchInput{
			BatchNo: "N" + itoa2(i), InspectedAt: atDay(100+i, 0), D1: 0}); err != nil {
			t.Fatal(err)
		}
	}
	d, _ = st.GetStreamDetail(ctx, sr.ID)
	if d.State.(statemachine.State).Severity != statemachine.Normal {
		t.Fatalf("severity=%s, flags-off replay must stay normal", d.State)
	}
	verifyAgainstReference(ctx, t, st, sr.ID)
}
