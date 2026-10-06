package statemachine

import (
	"fmt"

	"qcinspect/internal/sampling"
)

// Judge decides a lot against one plan. For single plans only d1 is used.
// For double plans it returns (accepted, secondTaken):
//
//	d1 <= c1            -> accept on first sample
//	d1 >= r1            -> reject on first sample
//	c1 < d1 < r1        -> second sample: accept iff d1+d2 <= c2
//
// A missing second sample when one is required is reported as an error so the
// API can point at the d2 field.
func Judge(pl sampling.Plan, d1, d2 int) (accepted, secondTaken bool, err error) {
	if d1 < 0 || d1 > pl.FirstSampleSize() {
		return false, false, fmt.Errorf("d1: must be within [0, %d]", pl.FirstSampleSize())
	}
	if pl.Kind == sampling.Single {
		return d1 <= pl.C, false, nil
	}
	if d1 <= pl.C1 {
		return true, false, nil
	}
	if d1 >= pl.R1 {
		return false, false, nil
	}
	if pl.N2 > 0 && d2 < 0 {
		return false, false, fmt.Errorf("d2: second sample required but no d2 provided")
	}
	if d2 > pl.N2 {
		return false, false, fmt.Errorf("d2: must be within [0, %d]", pl.N2)
	}
	return d1+d2 <= pl.C2, true, nil
}

// StepWithFlags folds one batch into the state. refs selects the plan for the
// current severity; fl carries the stream control flags.
func StepWithFlags(st State, b BatchInput, plans PlanTriple, refs map[Severity]PlanRef, fl FlagsExported) (State, BatchResult, error) {
	out := BatchResult{
		Ordinal:    st.Ordinal + 1,
		Severity:   st.Severity,
		Score:      st.Score,
		Transition: TransNone,
	}
	// Reduced -> normal when the stable flag is revoked (checked before the
	// lot is judged, so this lot is already judged under normal inspection —
	// GB/T 2828.1 treats the withdrawal of authority as immediate; the batch
	// itself is then inspected under normal rules).
	transitioning := false
	if st.Severity == Reduced && !fl.ProductionStable {
		st = enterNormal(st)
		out.Severity = Normal
		transitioning = true
	}
	if st.Severity == Suspended {
		return st, BatchResult{}, fmt.Errorf("%w: lot %q falls within a suspended period; record a resume first",
			ErrSuspended, b.BatchNo)
	}

	pl := planFor(plans, st.Severity)
	ref := refs[st.Severity]
	out.Plan = ref

	accepted, secondTaken, err := Judge(pl, b.D1, b.D2)
	if err != nil {
		return st, BatchResult{}, err
	}
	out.Accepted = accepted
	out.SecondTaken = secondTaken

	switch st.Severity {
	case Normal:
		// Switching score.
		st.Score = scoreAfter(st.Score, pl, accepted, b.D1, secondTaken)
		// Window of the last up-to-5 normal-run outcomes.
		st.NormalWindow = append(st.NormalWindow, accepted)
		if len(st.NormalWindow) > 5 {
			st.NormalWindow = st.NormalWindow[len(st.NormalWindow)-5:]
		}
		switch {
		case !accepted && countReject(st.NormalWindow) >= 2:
			st = enterTightened(st)
			out.Transition = TransToTightened
		case accepted && st.Score >= 30 && fl.ProductionStable && fl.SupervisorApproval:
			st = enterReduced(st)
			out.Transition = TransToReduced
		}
	case Tightened:
		if accepted {
			st.TightenedAcceptRun++
			if st.TightenedAcceptRun >= 5 {
				st = enterNormal(st)
				out.Transition = TransToNormal
			}
		} else {
			st.TightenedAcceptRun = 0
			st.TightenedRejectTotal++
			if st.TightenedRejectTotal >= 5 {
				st.Severity = Suspended
				out.Transition = TransSuspended
			}
		}
	case Reduced:
		if !accepted {
			st = enterNormal(st)
			out.Transition = TransToNormal
		}
	}

	if transitioning && out.Transition == TransNone {
		// Severity changed before judging due to revoked stability; mark the
		// visible transition so records show the switch.
		out.Transition = TransToNormal
	}
	out.Score = st.Score
	st.Ordinal++
	return st, out, nil
}

// scoreAfter applies the switching-score rule to one normal lot.
func scoreAfter(score int, pl sampling.Plan, accepted bool, d1 int, secondTaken bool) int {
	if !accepted {
		return 0 // not accepted resets the score
	}
	if pl.Kind == sampling.Double {
		if secondTaken {
			return score + 1 // accepted after second sample
		}
		// Accepted on first sample: score against (n1, c1) like a single plan.
		return score + singleBonus(pl.C1, d1)
	}
	return score + singleBonus(pl.C, d1)
}

func singleBonus(c, d int) int {
	switch {
	case d == 0:
		return 3
	case d == 1:
		return 2
	case c >= 2 && d >= 2:
		return 1
	default:
		return 0
	}
}

func countReject(window []bool) int {
	n := 0
	for _, acc := range window {
		if !acc {
			n++
		}
	}
	return n
}

func enterNormal(st State) State {
	st.Severity = Normal
	st.Score = 0
	st.NormalWindow = []bool{}
	st.TightenedAcceptRun = 0
	st.TightenedRejectTotal = 0
	return st
}

func enterTightened(st State) State {
	st.Severity = Tightened
	st.Score = 0
	st.NormalWindow = []bool{}
	st.TightenedAcceptRun = 0
	st.TightenedRejectTotal = 0
	return st
}

func enterReduced(st State) State {
	st.Severity = Reduced
	// Score keeps its value until a lot on reduced inspection ends the
	// reduced period; on return to normal a new score starts.
	return st
}

func planFor(plans PlanTriple, sev Severity) sampling.Plan {
	switch sev {
	case Tightened:
		return plans.Tightened
	case Reduced:
		return plans.Reduced
	default:
		return plans.Normal
	}
}
