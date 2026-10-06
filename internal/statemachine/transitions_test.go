package statemachine_test

import (
	"testing"

	"qcinspect/internal/statemachine"
)

var flOn = statemachine.FlagsExported{ProductionStable: true, SupervisorApproval: true}
var flOff = statemachine.FlagsExported{}

// Normal -> tightened: 2 rejects among <=5 consecutive normal lots.
func TestNormalToTightened(t *testing.T) {
	triple := testTriple()
	// reject, accept, reject -> tightened on the 3rd.
	res, st := fold(t, statemachine.InitialState(), triple, flOff, 3, 0, 3)
	if st.Severity != statemachine.Tightened {
		t.Fatalf("severity=%s", st.Severity)
	}
	if res[2].Severity != statemachine.Normal {
		t.Error("triggering batch must be judged under normal inspection")
	}
	if res[2].Transition != statemachine.TransToTightened {
		t.Errorf("transition=%s", res[2].Transition)
	}
	if st.Score != 0 || len(st.NormalWindow) != 0 {
		t.Error("normal counters must reset on entering tightened")
	}
}

// Two rejects more than 5 lots apart do NOT trigger tightened.
func TestNormalWindowSlides(t *testing.T) {
	triple := testTriple()
	// R, A*5, R : at the last reject the 5-lot window contains only 1 reject.
	seq := append([]int{3}, acceptSeq(5)...)
	seq = append(seq, 3)
	_, st := fold(t, statemachine.InitialState(), triple, flOff, seq...)
	if st.Severity != statemachine.Normal {
		t.Fatalf("severity=%s, want normal (window slid)", st.Severity)
	}
}

// Tightened -> normal after 5 consecutive accepted lots.
func TestTightenedToNormal(t *testing.T) {
	triple := testTriple()
	st := statemachine.State{
		Severity: statemachine.Tightened, NormalWindow: []bool{},
	}
	// On tightened (c=1) d=0 accepts.
	res, st := fold(t, st, triple, flOff, 0, 0, 0, 0, 0)
	if st.Severity != statemachine.Normal {
		t.Fatalf("severity=%s", st.Severity)
	}
	if res[4].Severity != statemachine.Tightened {
		t.Error("triggering batch judged under tightened")
	}
	if res[4].Transition != statemachine.TransToNormal {
		t.Error("expected transition marker")
	}
	// A reject breaks the consecutive-accept run.
	st2 := statemachine.State{Severity: statemachine.Tightened}
	r2, st2 := fold(t, st2, triple, flOff, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0)
	if st2.Severity != statemachine.Normal {
		t.Fatal("4 accept, reject, 5 accept should eventually return to normal")
	}
	if !r2[3].Accepted || r2[4].Accepted {
		t.Fatal("sanity: d=0 accepts, d=2 must reject on tightened")
	}
}

// Normal -> reduced once the switching score reaches 30 with both flags.
func TestNormalToReduced(t *testing.T) {
	triple := testTriple()
	// 10 accepted lots with d=0 -> +3 each -> exactly 30 on the 10th.
	res, st := fold(t, statemachine.InitialState(), triple, flOn, acceptSeq(10)...)
	if st.Severity != statemachine.Reduced {
		t.Fatalf("severity=%s", st.Severity)
	}
	if res[8].Score != 27 || res[9].Score != 30 {
		t.Errorf("score path wrong: %d then %d", res[8].Score, res[9].Score)
	}
	if res[9].Transition != statemachine.TransToReduced {
		t.Error("expected to_reduced on the 10th lot")
	}
	if res[8].Transition != statemachine.TransNone {
		t.Error("must not switch before the score reaches 30")
	}
}

// Without the stream flags, score 30 does NOT switch to reduced.
func TestNormalNoReducedWithoutFlags(t *testing.T) {
	triple := testTriple()
	_, st := fold(t, statemachine.InitialState(), triple, flOff, acceptSeq(10)...)
	if st.Severity != statemachine.Normal {
		t.Fatalf("severity=%s, want normal without flags", st.Severity)
	}
}

// Score add/reset rules.
func TestSwitchingScoreRules(t *testing.T) {
	triple := testTriple()
	// d=0:+3, d=1:+2, d=2:+1, reject:reset.
	res, st := fold(t, statemachine.InitialState(), triple, flOff, 0, 1, 2)
	if st.Score != 6 {
		t.Errorf("score after 0,1,2 = %d want 6", st.Score)
	}
	if res[0].Score != 3 || res[1].Score != 5 || res[2].Score != 6 {
		t.Errorf("per-lot score trail: %d %d %d", res[0].Score, res[1].Score, res[2].Score)
	}
	_, st2 := fold(t, st, triple, flOff, 3) // reject
	if st2.Score != 0 {
		t.Errorf("reject must reset score, got %d", st2.Score)
	}
}

// Reduced -> normal on one rejected lot.
func TestReducedToNormalOnReject(t *testing.T) {
	triple := testTriple()
	st := statemachine.State{Severity: statemachine.Reduced, Score: 30, NormalWindow: []bool{}}
	// reduced c=4: d=5 rejects.
	res, st := fold(t, st, triple, flOn, 5)
	if st.Severity != statemachine.Normal {
		t.Fatalf("severity=%s", st.Severity)
	}
	if res[0].Severity != statemachine.Reduced {
		t.Error("rejected lot judged under reduced inspection")
	}
	if res[0].Transition != statemachine.TransToNormal {
		t.Error("transition marker")
	}
	if st.Score != 0 {
		t.Error("returning to normal starts a fresh score")
	}
}

// Reduced -> normal immediately when the stability flag is withdrawn.
func TestReducedToNormalOnFlagRevoked(t *testing.T) {
	triple := testTriple()
	st := statemachine.State{Severity: statemachine.Reduced, Score: 30}
	fl := statemachine.FlagsExported{ProductionStable: false, SupervisorApproval: true}
	res, st := fold(t, st, triple, fl, 0) // accepted lot
	if st.Severity != statemachine.Normal {
		t.Fatalf("severity=%s", st.Severity)
	}
	if res[0].Severity != statemachine.Normal {
		t.Error("after withdrawal the lot is inspected under normal rules")
	}
	if res[0].Transition != statemachine.TransToNormal {
		t.Error("transition must be visible in the record")
	}
}

// Tightened -> suspended on 5 cumulative rejects; resume restarts tightened.
func TestTightenedSuspendAndResume(t *testing.T) {
	triple := testTriple()
	st := statemachine.State{Severity: statemachine.Tightened}
	// Alternate reject/accept: accept run never accumulates to 5.
	res, st := fold(t, st, triple, flOff, 2, 0, 2, 0, 2, 0, 2, 0, 2)
	if st.Severity != statemachine.Suspended {
		t.Fatalf("severity=%s", st.Severity)
	}
	if res[8].Transition != statemachine.TransSuspended {
		t.Error("expected suspension marker")
	}
	if st.TightenedRejectTotal != 5 {
		t.Errorf("reject total=%d", st.TightenedRejectTotal)
	}
	// Manual resume restarts on tightened with counters zeroed.
	st = statemachine.ResumeAsTightened(st)
	if st.Severity != statemachine.Tightened || st.TightenedRejectTotal != 0 ||
		st.TightenedAcceptRun != 0 {
		t.Fatal("resume must restart tightened with fresh counters")
	}
	_, st = fold(t, st, triple, flOff, acceptSeq(5)...)
	if st.Severity != statemachine.Normal {
		t.Fatal("after resume, 5 accepts must return to normal")
	}
}

// Suspended streams reject further lots (store enforces; state machine errors).
func TestSuspendedRejectsBatch(t *testing.T) {
	triple := testTriple()
	st := statemachine.State{Severity: statemachine.Suspended}
	_, _, err := statemachine.StepWithFlags(st,
		statemachine.BatchInput{BatchNo: "x", D1: 0},
		triple, refsFor(triple), flOn)
	if err == nil {
		t.Fatal("must not fold a lot while suspended")
	}
}
