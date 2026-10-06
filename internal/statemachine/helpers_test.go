package statemachine_test

import (
	"qcinspect/internal/sampling"
	"qcinspect/internal/statemachine"
	"testing"
)

// Plans used across transition tests (all single, n=5):
//
//	normal c=2, tightened c=1, reduced c=4
func testTriple() statemachine.PlanTriple {
	return statemachine.PlanTriple{
		Normal:    sampling.Plan{Kind: sampling.Single, N: 5, C: 2},
		Tightened: sampling.Plan{Kind: sampling.Single, N: 5, C: 1},
		Reduced:   sampling.Plan{Kind: sampling.Single, N: 5, C: 4},
	}
}

func refsFor(triple statemachine.PlanTriple) map[statemachine.Severity]statemachine.PlanRef {
	return map[statemachine.Severity]statemachine.PlanRef{
		statemachine.Normal:    {PlanID: 1, Name: "normal", Plan: triple.Normal},
		statemachine.Tightened: {PlanID: 2, Name: "tightened", Plan: triple.Tightened},
		statemachine.Reduced:   {PlanID: 3, Name: "reduced", Plan: triple.Reduced},
	}
}

// fold folds a list of (d1) results from the given start state.
func fold(t *testing.T, start statemachine.State, triple statemachine.PlanTriple,
	fl statemachine.FlagsExported, ds ...int) ([]statemachine.BatchResult, statemachine.State) {
	t.Helper()
	refs := refsFor(triple)
	st := start
	out := make([]statemachine.BatchResult, 0, len(ds))
	for i, d := range ds {
		next, r, err := statemachine.StepWithFlags(st,
			statemachine.BatchInput{ID: int64(i + 1), BatchNo: "b", D1: d},
			triple, refs, fl)
		if err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
		st = next
		out = append(out, r)
	}
	return out, st
}

func acceptSeq(n int) []int {
	out := make([]int, n) // all d=0, accepted under every plan
	return out
}
