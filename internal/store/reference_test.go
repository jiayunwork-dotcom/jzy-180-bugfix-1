package store_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"qcinspect/internal/replay"
	"qcinspect/internal/sampling"
	"qcinspect/internal/statemachine"
	"qcinspect/internal/store"
)

// refState is the independently computed reference: load every row from
// PostgreSQL and fold the complete timeline with replay.FullReplay.
type refState struct {
	entries []replay.Entry
	final   statemachine.State
	flags   replay.Flags
	triple  statemachine.PlanTriple
	refs    map[statemachine.Severity]statemachine.PlanRef
}

func loadReference(ctx context.Context, t *testing.T, st *store.Store, streamID int64) refState {
	t.Helper()
	pool := st.Pool()

	var nID, tID, rID int64
	var stable, approval bool
	var initStable, initApproval bool
	var stateRaw []byte
	err := pool.QueryRow(ctx,
		`SELECT normal_plan_id, tightened_plan_id, reduced_plan_id,
		        production_stable, supervisor_approval,
		        initial_production_stable, initial_supervisor_approval,
		        current_state
		 FROM streams WHERE id=$1`, streamID).
		Scan(&nID, &tID, &rID, &stable, &approval, &initStable, &initApproval, &stateRaw)
	if err != nil {
		t.Fatal(err)
	}
	loadPlan := func(id int64) (sampling.Plan, string) {
		var def []byte
		var name string
		if err := pool.QueryRow(ctx, `SELECT definition, name FROM plans WHERE id=$1`, id).
			Scan(&def, &name); err != nil {
			t.Fatal(err)
		}
		var pl sampling.Plan
		if err := json.Unmarshal(def, &pl); err != nil {
			t.Fatal(err)
		}
		return pl, name
	}
	pn, nn := loadPlan(nID)
	pt, nt := loadPlan(tID)
	pr, nr := loadPlan(rID)
	triple := statemachine.PlanTriple{Normal: pn, Tightened: pt, Reduced: pr}
	refs := map[statemachine.Severity]statemachine.PlanRef{
		statemachine.Normal:    {PlanID: nID, Name: nn, Plan: pn},
		statemachine.Tightened: {PlanID: tID, Name: nt, Plan: pt},
		statemachine.Reduced:   {PlanID: rID, Name: nr, Plan: pr},
	}

	batchRows, err := pool.Query(ctx,
		`SELECT id, batch_no, inspected_at, d1, d2 FROM batches WHERE stream_id=$1`, streamID)
	if err != nil {
		t.Fatal(err)
	}
	type br struct {
		id int64
		no string
		tm time.Time
		d1 int
		d2 int
	}
	var braws []br
	for batchRows.Next() {
		var b br
		var d2 *int
		if err := batchRows.Scan(&b.id, &b.no, &b.tm, &b.d1, &d2); err != nil {
			t.Fatal(err)
		}
		if d2 != nil {
			b.d2 = *d2
		}
		braws = append(braws, b)
	}
	batchRows.Close()

	eventRows, err := pool.Query(ctx,
		`SELECT id, kind, occurred_at, production_stable, supervisor_approval
		 FROM stream_events
		 WHERE stream_id=$1 ORDER BY occurred_at, id`, streamID)
	if err != nil {
		t.Fatal(err)
	}
	type er struct {
		id     int64
		kind   string
		tm     time.Time
		stable *bool
		appr   *bool
	}
	var eraws []er
	for eventRows.Next() {
		var e er
		if err := eventRows.Scan(&e.id, &e.kind, &e.tm, &e.stable, &e.appr); err != nil {
			t.Fatal(err)
		}
		eraws = append(eraws, e)
	}
	eventRows.Close()

	tl := replay.Timeline{}
	for _, b := range braws {
		tl.Batches = append(tl.Batches, statemachine.BatchInput{
			ID:          b.id,
			BatchNo:     b.no,
			InspectedAt: b.tm.UTC().Format(time.RFC3339Nano),
			D1:          b.d1,
			D2:          b.d2,
		})
	}
	for _, e := range eraws {
		switch e.kind {
		case "resume":
			tl.Events = append(tl.Events, replay.ResumeEvent{
				ID:         e.id,
				OccurredAt: e.tm.UTC().Format(time.RFC3339Nano),
			})
		case "flags":
			if e.stable == nil || e.appr == nil {
				t.Fatalf("flags event %d missing flag columns", e.id)
			}
			tl.FlagEvents = append(tl.FlagEvents, replay.FlagEvent{
				ID:                 e.id,
				OccurredAt:         e.tm.UTC().Format(time.RFC3339Nano),
				ProductionStable:   *e.stable,
				SupervisorApproval: *e.appr,
			})
		}
	}
	// Replay seeds from the stream's INITIAL flags; flag events carry the
	// changes. The stream row's current flags must equal the value of the
	// last flag event in replay order (with same-boundary flag events ordered
	// newest-first, that is simply the greatest (occurred_at,id) one), or the
	// initial flags when there are no flag events.
	fl := replay.Flags{ProductionStable: initStable, SupervisorApproval: initApproval}
	entries, final, err := replay.FullReplay(triple, refs, fl, tl)
	if err != nil {
		// The reference should never error on persisted data: that itself
		// signals a serious inconsistency (e.g. a lot stored while suspended).
		t.Fatalf("reference full replay failed: %v", err)
	}
	current := fl
	for i := len(entries) - 1; i >= 0; i-- {
		if entries[i].Kind == "flags" {
			current = entries[i].FlagsAfter
			break
		}
	}
	if current.ProductionStable != stable || current.SupervisorApproval != approval {
		t.Fatalf("stream current flags (%v,%v) != timeline-derived (%v,%v); initial=(%v,%v) flagEvents=%+v",
			stable, approval, current.ProductionStable, current.SupervisorApproval,
			initStable, initApproval, tl.FlagEvents)
	}
	var storedFinal statemachine.State
	if err := json.Unmarshal(stateRaw, &storedFinal); err != nil {
		t.Fatal(err)
	}
	if !statesEqual(final, storedFinal) {
		t.Fatalf("stored current state != full replay:\nstored=%+v\nreplay=%+v",
			storedFinal, final)
	}
	return refState{
		entries: entries, final: final, flags: fl,
		triple: triple, refs: refs,
	}
}

// verifyAgainstReference compares every persisted batch record (severity,
// decision, score, transition, plan id, ordinal) with a fresh full replay.
func verifyAgainstReference(ctx context.Context, t *testing.T, st *store.Store, streamID int64) refState {
	t.Helper()
	ref := loadReference(ctx, t, st, streamID)

	rows, err := st.ListBatches(ctx, streamID)
	if err != nil {
		t.Fatal(err)
	}
	refByID := map[int64]statemachine.BatchResult{}
	for _, e := range ref.entries {
		if !e.IsEvent {
			refByID[e.BatchID] = e.Result
		}
	}
	if len(rows) != len(refByID) {
		t.Fatalf("batch count: stored %d reference %d", len(rows), len(refByID))
	}
	for _, row := range rows {
		want, ok := refByID[row.ID]
		if !ok {
			t.Fatalf("stored batch %d absent from reference timeline", row.ID)
		}
		if gotSev, _ := row.Result["severity"].(string); gotSev != string(want.Severity) {
			t.Errorf("batch %d severity stored=%s want=%s", row.ID, gotSev, want.Severity)
		}
		gotAcc, _ := row.Result["accepted"].(bool)
		if gotAcc != want.Accepted {
			t.Errorf("batch %d accepted stored=%v want=%v", row.ID, gotAcc, want.Accepted)
		}
		gotSecond, _ := row.Result["second_sample_taken"].(bool)
		if gotSecond != want.SecondTaken {
			t.Errorf("batch %d second_sample_taken stored=%v want=%v", row.ID, gotSecond, want.SecondTaken)
		}
		gotScore, _ := row.Result["switching_score"].(float64)
		if int(gotScore) != want.Score {
			t.Errorf("batch %d score stored=%v want=%d", row.ID, gotScore, want.Score)
		}
		gotTr, _ := row.Result["transition"].(string)
		if gotTr != string(want.Transition) {
			t.Errorf("batch %d transition stored=%s want=%s", row.ID, gotTr, want.Transition)
		}
		gotOrd, _ := row.Result["ordinal"].(float64)
		if int(gotOrd) != want.Ordinal {
			t.Errorf("batch %d ordinal stored=%v want=%d", row.ID, gotOrd, want.Ordinal)
		}
		// Full plan snapshot comparison (which plan档 judged the lot matters
		// for the "台账与系统一致" requirement).
		planObj, ok := row.Result["plan"].(map[string]interface{})
		if !ok {
			t.Errorf("batch %d missing plan snapshot", row.ID)
			continue
		}
		gotPlanID, _ := planObj["plan_id"].(float64)
		if int64(gotPlanID) != want.Plan.PlanID {
			t.Errorf("batch %d plan_id stored=%v want=%d", row.ID, gotPlanID, want.Plan.PlanID)
		}
		gotName, _ := planObj["name"].(string)
		if gotName != want.Plan.Name {
			t.Errorf("batch %d plan name stored=%q want=%q", row.ID, gotName, want.Plan.Name)
		}
		planDef, ok := planObj["plan"].(map[string]interface{})
		if !ok {
			t.Errorf("batch %d plan snapshot missing plan definition", row.ID)
			continue
		}
		if gotKind, _ := planDef["kind"].(string); gotKind != string(want.Plan.Plan.Kind) {
			t.Errorf("batch %d plan kind stored=%q want=%q", row.ID, gotKind, want.Plan.Plan.Kind)
		}
		if gotN, _ := planDef["n"].(float64); int(gotN) != want.Plan.Plan.N {
			t.Errorf("batch %d plan n stored=%v want=%d", row.ID, gotN, want.Plan.Plan.N)
		}
		if gotC, _ := planDef["c"].(float64); int(gotC) != want.Plan.Plan.C {
			t.Errorf("batch %d plan c stored=%v want=%d", row.ID, gotC, want.Plan.Plan.C)
		}
	}
	return ref
}

func statesEqual(a, b statemachine.State) bool {
	if a.Severity != b.Severity || a.Score != b.Score ||
		a.TightenedAcceptRun != b.TightenedAcceptRun ||
		a.TightenedRejectTotal != b.TightenedRejectTotal ||
		a.Ordinal != b.Ordinal {
		return false
	}
	if len(a.NormalWindow) != len(b.NormalWindow) {
		return false
	}
	for i := range a.NormalWindow {
		if a.NormalWindow[i] != b.NormalWindow[i] {
			return false
		}
	}
	return true
}
