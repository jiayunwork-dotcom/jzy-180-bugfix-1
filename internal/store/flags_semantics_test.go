package store_test

import (
	"context"
	"math/rand"
	"sort"
	"testing"
	"time"

	"qcinspect/internal/replay"
	"qcinspect/internal/sampling"
	"qcinspect/internal/statemachine"
	"qcinspect/internal/store"
)

// mkFlagPlans builds the user's reproduction triple:
// normal n=10,c=0; tightened n=10,c=0; reduced n=5,c=0 (all single).
func mkFlagPlans(t *testing.T, st *store.Store, suffix string) (int64, int64, int64) {
	n := mustPlan(t, st, "f_normal_"+suffix, sampling.Plan{Kind: sampling.Single, N: 10, C: 0})
	ti := mustPlan(t, st, "f_tight_"+suffix, sampling.Plan{Kind: sampling.Single, N: 10, C: 0})
	r := mustPlan(t, st, "f_reduced_"+suffix, sampling.Plan{Kind: sampling.Single, N: 5, C: 0})
	return n, ti, r
}

func flagAt(hour int) time.Time {
	return time.Date(2026, 3, 1, hour, 0, 0, 0, time.UTC)
}

// byBatchNo indexes batch views by lot number.
func byBatchNo(bs []store.BatchView) map[string]store.BatchView {
	m := make(map[string]store.BatchView, len(bs))
	for _, b := range bs {
		m[b.BatchNo] = b
	}
	return m
}

// TestFlagsReproduction is the exact scenario from the bug report:
// L1..L12 judged normal with both flags off; PATCH both flags on must not
// rewrite a single batch, current state stays normal with score 36; the next
// lot L13 is still judged normal, reaches the threshold under the new flags
// and switches; L14 onwards is reduced.
func TestFlagsReproduction(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	nID, tID, rID := mkFlagPlans(t, st, "rep")
	sr, err := st.CreateStream(ctx, store.CreateStreamParams{
		Name: "flag-repro", NormalPlanID: nID, TightenedPlanID: tID, ReducedPlanID: rID,
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 12; i++ {
		if _, err := st.AddBatch(ctx, sr.ID, store.BatchInput{
			BatchNo: "L" + itoa2(i), InspectedAt: flagAt(i), D1: 0,
		}); err != nil {
			t.Fatal(err)
		}
	}
	detail, err := st.GetStreamDetail(ctx, sr.ID)
	if err != nil {
		t.Fatal(err)
	}
	cur := detail.State.(statemachine.State)
	if cur.Severity != statemachine.Normal || cur.Score != 36 {
		t.Fatalf("pre-patch state=%s score=%d, want normal/36", cur.Severity, cur.Score)
	}

	mr, err := st.SetFlags(ctx, sr.ID, true, true)
	if err != nil {
		t.Fatal(err)
	}
	// Current state unchanged by the flag change itself.
	if mr.State.Severity != statemachine.Normal || mr.State.Score != 36 {
		t.Fatalf("post-patch state=%s score=%d, want normal/36", mr.State.Severity, mr.State.Score)
	}
	bm := byBatchNo(mr.All)
	for i := 1; i <= 12; i++ {
		b := bm["L"+itoa2(i)]
		if got := b.Result["severity"]; got != "normal" {
			t.Fatalf("L%d severity=%v, must stay normal after flag patch", i, got)
		}
		if tr := b.Result["transition"]; tr != "none" {
			t.Fatalf("L%d transition=%v, must stay none", i, tr)
		}
		if sc := b.Result["switching_score"].(float64); int(sc) != 3*i {
			t.Fatalf("L%d score=%v want %d", i, sc, 3*i)
		}
		planObj := b.Result["plan"].(map[string]interface{})
		if planObj["plan_id"].(float64) != float64(nID) {
			t.Fatalf("L%d used plan=%v, must stay the normal plan", i, planObj["plan_id"])
		}
		if n := planObj["plan"].(map[string]interface{})["n"].(float64); int(n) != 10 {
			t.Fatalf("L%d used n=%v want 10", i, n)
		}
		if acc := b.Result["accepted"].(bool); !acc {
			t.Fatalf("L%d accepted=false, want true", i)
		}
	}

	// L13 (13:00, d=0): still judged under the normal plan; accepted; the
	// accumulated score already clears 30 and both flags are on -> switch to
	// reduced takes effect AFTER L13.
	mr, err = st.AddBatch(ctx, sr.ID, store.BatchInput{
		BatchNo: "L13", InspectedAt: flagAt(13), D1: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	b13 := byBatchNo(mr.All)["L13"]
	if b13.Result["severity"] != "normal" {
		t.Fatalf("L13 severity=%v want normal (switch fires after it)", b13.Result["severity"])
	}
	if b13.Result["transition"] != "to_reduced" {
		t.Fatalf("L13 transition=%v want to_reduced", b13.Result["transition"])
	}
	if mr.State.Severity != statemachine.Reduced {
		t.Fatalf("state after L13=%s want reduced", mr.State.Severity)
	}

	// L14: judged under the reduced plan n=5.
	mr, err = st.AddBatch(ctx, sr.ID, store.BatchInput{
		BatchNo: "L14", InspectedAt: flagAt(14), D1: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	b14 := byBatchNo(mr.All)["L14"]
	if b14.Result["severity"] != "reduced" {
		t.Fatalf("L14 severity=%v want reduced", b14.Result["severity"])
	}
	planObj := b14.Result["plan"].(map[string]interface{})
	if planObj["plan_id"].(float64) != float64(rID) ||
		planObj["plan"].(map[string]interface{})["n"].(float64) != 5 {
		t.Fatalf("L14 plan=%v want reduced plan (id=%d n=5)", planObj, rID)
	}
	if mr.State.Severity != statemachine.Reduced {
		t.Fatalf("state after L14=%s want reduced", mr.State.Severity)
	}
}

// TestFlagsRevocation: switch to reduced, then revoke production_stable.
// Already-judged reduced batches stay reduced; the first batch entered after
// the revocation is judged normal.
func TestFlagsRevocation(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	nID, tID, rID := mkFlagPlans(t, st, "rev")
	// Flags already on at stream creation: in force from the start.
	sr, err := st.CreateStream(ctx, store.CreateStreamParams{
		Name: "flag-rev", NormalPlanID: nID, TightenedPlanID: tID, ReducedPlanID: rID,
		ProductionStable: true, SupervisorApproval: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 10; i++ {
		if _, err := st.AddBatch(ctx, sr.ID, store.BatchInput{
			BatchNo: "A" + itoa2(i), InspectedAt: flagAt(i), D1: 0,
		}); err != nil {
			t.Fatal(err)
		}
	}
	// A10 switches to reduced; A11..A13 are reduced.
	var mr store.MutationResult
	for i := 11; i <= 13; i++ {
		mr, err = st.AddBatch(ctx, sr.ID, store.BatchInput{
			BatchNo: "A" + itoa2(i), InspectedAt: flagAt(i), D1: 0,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if mr.State.Severity != statemachine.Reduced {
		t.Fatalf("precondition: severity=%s want reduced", mr.State.Severity)
	}

	// Revoke production stability at the boundary (13:00).
	mr, err = st.SetFlags(ctx, sr.ID, false, true)
	if err != nil {
		t.Fatal(err)
	}
	bm := byBatchNo(mr.All)
	for i := 1; i <= 10; i++ {
		if bm["A"+itoa2(i)].Result["severity"] != "normal" {
			t.Fatalf("A%d must remain normal after revocation", i)
		}
	}
	for i := 11; i <= 13; i++ {
		b := bm["A"+itoa2(i)]
		if b.Result["severity"] != "reduced" {
			t.Fatalf("A%d severity=%v, reduced lots before the boundary must stay reduced", i, b.Result["severity"])
		}
		if b.Result["transition"] != "none" {
			t.Fatalf("A%d transition=%v, historical record must not be rewritten", i, b.Result["transition"])
		}
	}
	// The fold applies the reduced->normal switch lazily at the first batch
	// after the boundary: that lot is judged under normal inspection and
	// carries the visible transition.
	mr, err = st.AddBatch(ctx, sr.ID, store.BatchInput{
		BatchNo: "A14", InspectedAt: flagAt(14), D1: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	b14 := byBatchNo(mr.All)["A14"]
	if b14.Result["severity"] != "normal" {
		t.Fatalf("A14 severity=%v want normal after revocation", b14.Result["severity"])
	}
	if b14.Result["transition"] != "to_normal" {
		t.Fatalf("A14 transition=%v want to_normal", b14.Result["transition"])
	}
	if mr.State.Severity != statemachine.Normal || mr.State.Score != 3 {
		t.Fatalf("state after A14=%s score=%d want normal/3", mr.State.Severity, mr.State.Score)
	}
	// Historical reduced batches are still untouched.
	if byBatchNo(mr.All)["A13"].Result["severity"] != "reduced" {
		t.Fatal("A13 must remain reduced")
	}
}

// TestFlagsBackfillBeforeBoundary: after a flag change, a lot backdated to a
// time before the boundary is judged with the flags in force at that time; a
// lot at exactly the boundary instant also precedes the flag event (the
// batch-before-event tie break).
func TestFlagsBackfillBeforeBoundary(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	nID, tID, rID := mkFlagPlans(t, st, "bf")
	sr, err := st.CreateStream(ctx, store.CreateStreamParams{
		Name: "flag-bf", NormalPlanID: nID, TightenedPlanID: tID, ReducedPlanID: rID,
	})
	if err != nil {
		t.Fatal(err)
	}
	// 9 normal perfect lots (score 27), hours 1..9, flags off.
	for i := 1; i <= 9; i++ {
		if _, err := st.AddBatch(ctx, sr.ID, store.BatchInput{
			BatchNo: "B" + itoa2(i), InspectedAt: flagAt(i), D1: 0,
		}); err != nil {
			t.Fatal(err)
		}
	}
	// Flags on at boundary 09:00.
	if _, err := st.SetFlags(ctx, sr.ID, true, true); err != nil {
		t.Fatal(err)
	}
	// Backfill B10 at 08:30 (strictly before the boundary, but after B8 at
	// 08:00). Folding order: B1..B8 (score 24, flags off), B10 (score 27,
	// flags still off -> no switch), B9 at 09:00 is the boundary lot and
	// orders before the flag event, so it is judged with the old flags too
	// (score 30, still no switch). Only a lot strictly after the boundary is
	// judged with the new flags: B11 at 09:30 scores 33 with both flags on
	// and carries to_reduced, itself judged normal.
	backfillAt := time.Date(2026, 3, 1, 8, 30, 0, 0, time.UTC)
	mr, err := st.AddBatch(ctx, sr.ID, store.BatchInput{
		BatchNo: "B10", InspectedAt: backfillAt, D1: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	b10 := byBatchNo(mr.All)["B10"]
	if b10.Result["severity"] != "normal" {
		t.Fatalf("backfilled B10 severity=%v want normal (flags off at 08:30)", b10.Result["severity"])
	}
	if b10.Result["transition"] != "none" {
		t.Fatalf("backfilled B10 transition=%v want none (flags not yet on)", b10.Result["transition"])
	}
	if sc := b10.Result["switching_score"].(float64); int(sc) != 27 {
		t.Fatalf("backfilled B10 score=%v want 27 (9th chronological lot, flags off)", sc)
	}
	b9 := byBatchNo(mr.All)["B9"]
	if b9.Result["severity"] != "normal" {
		t.Fatal("B9 must be judged normal")
	}
	if b9.Result["transition"] != "none" {
		t.Fatalf("boundary lot B9 transition=%v want none (flags take effect only after it)",
			b9.Result["transition"])
	}
	if mr.State.Severity != statemachine.Normal || mr.State.Score != 30 {
		t.Fatalf("state after backfill=%s score=%d want normal/30",
			mr.State.Severity, mr.State.Score)
	}

	// B11 strictly after the boundary: normal judgment, then to_reduced.
	mr, err = st.AddBatch(ctx, sr.ID, store.BatchInput{
		BatchNo: "B11", InspectedAt: time.Date(2026, 3, 1, 9, 30, 0, 0, time.UTC), D1: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	b11 := byBatchNo(mr.All)["B11"]
	if b11.Result["severity"] != "normal" || b11.Result["transition"] != "to_reduced" {
		t.Fatalf("B11 sev=%v tr=%v want normal/to_reduced",
			b11.Result["severity"], b11.Result["transition"])
	}
	if mr.State.Severity != statemachine.Reduced {
		t.Fatalf("state after B11=%s want reduced", mr.State.Severity)
	}

	// A lot backfilled at EXACTLY the boundary instant also precedes the flag
	// event (batch-before-event tie break) and keeps the flags in force up to
	// that instant.
	sr2, _ := st.CreateStream(ctx, store.CreateStreamParams{
		Name: "flag-bf2", NormalPlanID: nID, TightenedPlanID: tID, ReducedPlanID: rID,
	})
	for i := 1; i <= 9; i++ {
		if _, err := st.AddBatch(ctx, sr2.ID, store.BatchInput{
			BatchNo: "C" + itoa2(i), InspectedAt: flagAt(100 + i), D1: 0,
		}); err != nil {
			t.Fatal(err)
		}
	}
	// Boundary = 109:00 (latest batch C9).
	if _, err := st.SetFlags(ctx, sr2.ID, true, true); err != nil {
		t.Fatal(err)
	}
	// Backfill at exactly the boundary time. At 109:00 the order is
	// C9 (older batch id), C10 (new batch), then the flag event. Both batches
	// are therefore judged with flags OFF: C9 stays at 27 without switching,
	// C10 reaches 30 but must not switch either; the current state stays
	// normal.
	mr, err = st.AddBatch(ctx, sr2.ID, store.BatchInput{
		BatchNo: "C10", InspectedAt: flagAt(109), D1: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	c10 := byBatchNo(mr.All)["C10"]
	if c10.Result["severity"] != "normal" {
		t.Fatalf("same-instant backfill C10 severity=%v want normal", c10.Result["severity"])
	}
	if c10.Result["transition"] != "none" {
		t.Fatalf("same-instant backfill C10 transition=%v want none", c10.Result["transition"])
	}
	if mr.State.Severity != statemachine.Normal || mr.State.Score != 30 {
		t.Fatalf("after same-instant backfill state=%s score=%d want normal/30",
			mr.State.Severity, mr.State.Score)
	}
}

// TestFlagsAtCreationAndOnEmptyStream: flags given at creation, or PATCHed
// while the stream still has no batches, apply from the timeline start.
func TestFlagsAtCreationAndOnEmptyStream(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	nID, tID, rID := mkFlagPlans(t, st, "ini")

	// Flags on at creation: the 10th lot switches to reduced.
	sr, err := st.CreateStream(ctx, store.CreateStreamParams{
		Name: "ini-create", NormalPlanID: nID, TightenedPlanID: tID, ReducedPlanID: rID,
		ProductionStable: true, SupervisorApproval: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	var mr store.MutationResult
	for i := 1; i <= 10; i++ {
		mr, err = st.AddBatch(ctx, sr.ID, store.BatchInput{
			BatchNo: "D" + itoa2(i), InspectedAt: flagAt(i), D1: 0,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if mr.State.Severity != statemachine.Reduced {
		t.Fatalf("flags at creation must apply from the start: severity=%s", mr.State.Severity)
	}

	// Empty stream PATCH: applies from the start once batches arrive.
	sr2, err := st.CreateStream(ctx, store.CreateStreamParams{
		Name: "ini-empty", NormalPlanID: nID, TightenedPlanID: tID, ReducedPlanID: rID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.SetFlags(ctx, sr2.ID, true, true); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 10; i++ {
		mr, err = st.AddBatch(ctx, sr2.ID, store.BatchInput{
			BatchNo: "E" + itoa2(i), InspectedAt: flagAt(i), D1: 0,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if mr.State.Severity != statemachine.Reduced {
		t.Fatalf("flags set on empty stream must apply from the start: severity=%s", mr.State.Severity)
	}
}

// refFlagEvt / refResumeEvt are the test-side mirror of administrative events.
type refFlagEvt struct {
	id                 int64
	hour               int
	productionStable   bool
	supervisorApproval bool
}

type refResumeEvt struct {
	id   int64
	hour int
}

type refBatch struct {
	id     int64
	no     string
	hour   int
	d1, d2 int
}

// TestRandomFlagEdits drives a long randomized sequence of in-order inserts,
// backdated inserts, count/time edits, deletes, resumes and repeated FLAG
// TOGGLES interleaved with everything. After every mutation it compares every
// persisted field and the current state against a full replay of a reference
// timeline maintained independently in the test.
func TestRandomFlagEdits(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	nID, tID, rID := mkFlagPlans(t, st, "rndflags")
	// Reduced n=5,c=0 rejects on d>=1, so reduced runs often fall straight
	// back to normal and that transition gets exercised too.
	sr, err := st.CreateStream(ctx, store.CreateStreamParams{
		Name: "rand-flags", NormalPlanID: nID, TightenedPlanID: tID, ReducedPlanID: rID,
	})
	if err != nil {
		t.Fatal(err)
	}
	sid := sr.ID

	var batches []refBatch
	var resumes []refResumeEvt
	var flagEvents []refFlagEvt
	initFlags := replay.Flags{}
	curFlags := initFlags

	rng := rand.New(rand.NewSource(99))
	no := 0
	newLot := func() string { no++; return "R" + itoa2(no) }

	syncEventIDs := func() {
		// Re-zip reference event ids against the database, in replay order
		// (occurred_at, batches irrelevant here, resume < flags, then
		// ascending event id).
		rows, qerr := st.Pool().Query(ctx,
			`SELECT id, kind, occurred_at FROM stream_events
			 WHERE stream_id=$1
			 ORDER BY occurred_at,
			          CASE kind WHEN 'resume' THEN 1 WHEN 'flags' THEN 2 ELSE 9 END,
			          id`,
			sid)
		if qerr != nil {
			t.Fatal(qerr)
		}
		type dbEv struct {
			id   int64
			kind byte // 'r' resume, 'f' flags
			hour int
		}
		hourOf := func(tm time.Time) int {
			return int(tm.UTC().Sub(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)) / time.Hour)
		}
		var dbEvents []dbEv
		for rows.Next() {
			var id int64
			var kind string
			var at0 time.Time
			if err := rows.Scan(&id, &kind, &at0); err != nil {
				t.Fatal(err)
			}
			k := byte('f')
			if kind == "resume" {
				k = 'r'
			}
			dbEvents = append(dbEvents, dbEv{id: id, kind: k, hour: hourOf(at0)})
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		type refEv struct {
			kind byte
			hour int
			seq  int // index inside its kind-specific local slice
		}
		var refEvents []refEv
		for i, e := range resumes {
			refEvents = append(refEvents, refEv{kind: 'r', hour: e.hour, seq: i})
		}
		for i, e := range flagEvents {
			refEvents = append(refEvents, refEv{kind: 'f', hour: e.hour, seq: i})
		}
		// Same ordering as replay: time, resume < flags, then ascending append
		// sequence (which equals ascending event id).
		sort.SliceStable(refEvents, func(i, j int) bool {
			a, b := refEvents[i], refEvents[j]
			if a.hour != b.hour {
				return a.hour < b.hour
			}
			if a.kind != b.kind {
				return a.kind < b.kind
			}
			return a.seq < b.seq
		})
		if len(dbEvents) != len(refEvents) {
			t.Fatalf("event count mismatch: ref=%d db=%d", len(refEvents), len(dbEvents))
		}
		for i, d := range dbEvents {
			r := refEvents[i]
			if d.hour != r.hour || d.kind != r.kind {
				t.Fatalf("event %d mismatch: db=(%c h=%d) ref=(%c h=%d)",
					i, d.kind, d.hour, r.kind, r.hour)
			}
			if r.kind == 'r' {
				resumes[r.seq].id = d.id
			} else {
				flagEvents[r.seq].id = d.id
			}
		}
	}

	rebuildAndCheck := func() {
		t.Helper()
		syncEventIDs()
		tl := replay.Timeline{}
		for _, b := range batches {
			tl.Batches = append(tl.Batches, statemachine.BatchInput{
				ID: b.id, BatchNo: b.no,
				InspectedAt: at(b.hour).Format(time.RFC3339Nano),
				D1:          b.d1, D2: b.d2,
			})
		}
		for _, e := range resumes {
			tl.Events = append(tl.Events, replay.ResumeEvent{
				ID: e.id, OccurredAt: at(e.hour).Format(time.RFC3339Nano),
			})
		}
		for _, f := range flagEvents {
			tl.FlagEvents = append(tl.FlagEvents, replay.FlagEvent{
				ID: f.id, OccurredAt: at(f.hour).Format(time.RFC3339Nano),
				ProductionStable:   f.productionStable,
				SupervisorApproval: f.supervisorApproval,
			})
		}
		triple := statemachine.PlanTriple{
			Normal:    sampling.Plan{Kind: sampling.Single, N: 10, C: 0},
			Tightened: sampling.Plan{Kind: sampling.Single, N: 10, C: 0},
			Reduced:   sampling.Plan{Kind: sampling.Single, N: 5, C: 0},
		}
		refs := map[statemachine.Severity]statemachine.PlanRef{
			statemachine.Normal:    {PlanID: nID, Name: "f_normal_rndflags", Plan: triple.Normal},
			statemachine.Tightened: {PlanID: tID, Name: "f_tight_rndflags", Plan: triple.Tightened},
			statemachine.Reduced:   {PlanID: rID, Name: "f_reduced_rndflags", Plan: triple.Reduced},
		}
		entries, final, rerr := replay.FullReplay(triple, refs, initFlags, tl)
		if rerr != nil {
			t.Fatalf("reference replay error: %v", rerr)
		}
		detail, derr := st.GetStreamDetail(ctx, sid)
		if derr != nil {
			t.Fatal(derr)
		}
		stored := detail.State.(statemachine.State)
		if !statesEqual(final, stored) {
			t.Fatalf("current state mismatch:\nreplay=%+v\nstored=%+v", final, stored)
		}
		wantByID := map[int64]statemachine.BatchResult{}
		for _, e := range entries {
			if !e.IsEvent {
				wantByID[e.BatchID] = e.Result
			}
		}
		rows, err := st.ListBatches(ctx, sid)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != len(wantByID) {
			t.Fatalf("batch count stored=%d replay=%d", len(rows), len(wantByID))
		}
		for _, row := range rows {
			want, ok := wantByID[row.ID]
			if !ok {
				t.Fatalf("stored batch %d absent in replay", row.ID)
			}
			if got, _ := row.Result["severity"].(string); got != string(want.Severity) {
				t.Fatalf("batch %d severity stored=%s want=%s", row.ID, got, want.Severity)
			}
			if got, _ := row.Result["accepted"].(bool); got != want.Accepted {
				t.Fatalf("batch %d accepted stored=%v want=%v", row.ID, got, want.Accepted)
			}
			if got, _ := row.Result["second_sample_taken"].(bool); got != want.SecondTaken {
				t.Fatalf("batch %d second stored=%v want=%v", row.ID, got, want.SecondTaken)
			}
			if got, _ := row.Result["switching_score"].(float64); int(got) != want.Score {
				t.Fatalf("batch %d score stored=%v want=%d", row.ID, got, want.Score)
			}
			if got, _ := row.Result["transition"].(string); got != string(want.Transition) {
				t.Fatalf("batch %d transition stored=%s want=%s", row.ID, got, want.Transition)
			}
			if got, _ := row.Result["ordinal"].(float64); int(got) != want.Ordinal {
				t.Fatalf("batch %d ordinal stored=%v want=%d", row.ID, got, want.Ordinal)
			}
			po, _ := row.Result["plan"].(map[string]interface{})
			if po == nil {
				t.Fatalf("batch %d missing plan snapshot", row.ID)
			}
			if got, _ := po["plan_id"].(float64); int64(got) != want.Plan.PlanID {
				t.Fatalf("batch %d plan_id stored=%v want=%d", row.ID, got, want.Plan.PlanID)
			}
			if got, _ := po["name"].(string); got != want.Plan.Name {
				t.Fatalf("batch %d plan name stored=%q want=%q", row.ID, got, want.Plan.Name)
			}
			pd, _ := po["plan"].(map[string]interface{})
			if got, _ := pd["n"].(float64); int(got) != want.Plan.Plan.N {
				t.Fatalf("batch %d plan n stored=%v want=%d", row.ID, got, want.Plan.Plan.N)
			}
			if got, _ := pd["c"].(float64); int(got) != want.Plan.Plan.C {
				t.Fatalf("batch %d plan c stored=%v want=%d", row.ID, got, want.Plan.Plan.C)
			}
		}
		// Current flags are replay-derived: the initial flags with flag events
		// applied in replay order (same-boundary events ascending id == append
		// order). A PATCH landing at an earlier boundary after deletions does
		// not necessarily win.
		wantCur := initFlags
		type fev struct {
			hour               int
			seq                int
			productionStable   bool
			supervisorApproval bool
		}
		var ordered []fev
		for i, f := range flagEvents {
			ordered = append(ordered, fev{
				hour: f.hour, seq: i,
				productionStable:   f.productionStable,
				supervisorApproval: f.supervisorApproval,
			})
		}
		sort.SliceStable(ordered, func(i, j int) bool {
			return ordered[i].hour < ordered[j].hour ||
				(ordered[i].hour == ordered[j].hour && ordered[i].seq < ordered[j].seq)
		})
		for _, f := range ordered {
			wantCur = replay.Flags{ProductionStable: f.productionStable, SupervisorApproval: f.supervisorApproval}
		}
		if detail.ProductionStable != wantCur.ProductionStable ||
			detail.SupervisorApproval != wantCur.SupervisorApproval {
			t.Fatalf("stream flags=(%v,%v) reference=(%v,%v)",
				detail.ProductionStable, detail.SupervisorApproval,
				wantCur.ProductionStable, wantCur.SupervisorApproval)
		}
	}

	for step := 0; step < 500; step++ {
		k := rng.Intn(100)
		switch {
		case k < 45: // insert, biased to in-order but often backdated
			hour := 1 + rng.Intn(400)
			// d in 0..5 (5 is reduced n=5 size; c=0 everywhere).
			d := rng.Intn(6)
			name := newLot()
			_, err := st.AddBatch(ctx, sid, store.BatchInput{
				BatchNo: name, InspectedAt: at(hour), D1: d,
			})
			if err == store.ErrSuspended {
				no-- // reuse the lot number
				continue
			}
			if err != nil {
				t.Fatalf("add: %v", err)
			}
			// Find its id from the DB mirror.
			var id int64
			if err := st.Pool().QueryRow(ctx,
				`SELECT id FROM batches WHERE stream_id=$1 AND batch_no=$2`, sid, name).
				Scan(&id); err != nil {
				t.Fatal(err)
			}
			batches = append(batches, refBatch{id: id, no: name, hour: hour, d1: d})
		case k < 60 && len(batches) > 0: // correct count
			i := rng.Intn(len(batches))
			newD := rng.Intn(6)
			if _, err := st.UpdateBatch(ctx, sid, batches[i].id, store.BatchInput{
				BatchNo: batches[i].no, InspectedAt: at(batches[i].hour), D1: newD,
			}); err == store.ErrSuspended {
				// rolled back
			} else if err != nil {
				t.Fatalf("update: %v", err)
			} else {
				batches[i].d1 = newD
			}
		case k < 72 && len(batches) > 0: // move in time
			i := rng.Intn(len(batches))
			newHour := 1 + rng.Intn(400)
			if _, err := st.UpdateBatch(ctx, sid, batches[i].id, store.BatchInput{
				BatchNo: batches[i].no, InspectedAt: at(newHour), D1: batches[i].d1,
			}); err == store.ErrSuspended {
				// rolled back
			} else if err != nil {
				t.Fatalf("move: %v", err)
			} else {
				batches[i].hour = newHour
			}
		case k < 82 && len(batches) > 0: // delete
			i := rng.Intn(len(batches))
			if _, err := st.DeleteBatch(ctx, sid, batches[i].id); err == store.ErrSuspended {
				// The deletion exposes an earlier suspended period; rolled
				// back.
			} else if err != nil {
				t.Fatalf("delete: %v", err)
			} else {
				batches = append(batches[:i], batches[i+1:]...)
			}
		case k < 92: // toggle flags — boundary is the latest batch hour
			ps := rng.Intn(2) == 0
			sa := rng.Intn(2) == 0
			if _, err := st.SetFlags(ctx, sid, ps, sa); err != nil {
				t.Fatalf("flags: %v", err)
			}
			curFlags = replay.Flags{ProductionStable: ps, SupervisorApproval: sa}
			if len(batches) > 0 {
				latest := 0
				for _, b := range batches {
					if b.hour > latest {
						latest = b.hour
					}
				}
				flagEvents = append(flagEvents, refFlagEvt{
					hour: latest, productionStable: ps, supervisorApproval: sa,
				})
			} else {
				// Empty stream: baseline rewritten from the start, no events.
				flagEvents = nil
				initFlags = curFlags
			}
		default: // resume after the newest batch when suspended
			detail, err := st.GetStreamDetail(ctx, sid)
			if err != nil {
				t.Fatal(err)
			}
			if detail.State.(statemachine.State).Severity != statemachine.Suspended {
				continue
			}
			latest := 0
			for _, b := range batches {
				if b.hour > latest {
					latest = b.hour
				}
			}
			hour := latest + 1
			if _, err := st.Resume(ctx, sid, at(hour)); err != nil {
				t.Fatalf("resume: %v", err)
			}
			resumes = append(resumes, refResumeEvt{hour: hour})
		}
		rebuildAndCheck()
	}
}
