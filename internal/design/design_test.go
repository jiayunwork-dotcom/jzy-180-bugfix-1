package design_test

import (
	"errors"
	"testing"

	"qcinspect/internal/design"
	"qcinspect/internal/sampling"
)

func meets(pl sampling.Plan, p design.Params) (okA, okL bool) {
	paA, _, _ := pl.Pa(p.AQL)
	paL, _, _ := pl.Pa(p.LTPD)
	return paA >= 1-p.Alpha-1e-9, paL <= p.Beta+1e-9
}

func TestSingleDesignMeetsConstraints(t *testing.T) {
	cases := []design.Params{
		{AQL: 0.01, Alpha: 0.05, LTPD: 0.06, Beta: 0.10},
		{AQL: 0.02, Alpha: 0.05, LTPD: 0.08, Beta: 0.10},
		{AQL: 0.005, Alpha: 0.01, LTPD: 0.03, Beta: 0.05},
		{AQL: 0.01, Alpha: 0.10, LTPD: 0.10, Beta: 0.05},
	}
	for i, p := range cases {
		pl, err := design.Single(p, design.Limits{})
		if err != nil {
			t.Fatalf("case %d: %v", i, err)
		}
		okA, okL := meets(pl, p)
		if !okA || !okL {
			t.Errorf("case %d plan n=%d c=%d fails constraints (%v,%v)", i, pl.N, pl.C, okA, okL)
		}
		// n-1 must have NO feasible c.
		for c := 0; c < pl.N; c++ {
			cand := sampling.Plan{Kind: sampling.Single, N: pl.N - 1, C: c}
			if a, l := meets(cand, p); a && l {
				t.Errorf("case %d: n=%d c=%d also works, so n=%d not minimal", i, pl.N-1, c, pl.N)
			}
		}
		// c minimal at this n: any smaller c fails the AQL constraint.
		if pl.C > 0 {
			smaller := sampling.Plan{Kind: sampling.Single, N: pl.N, C: pl.C - 1}
			paA, _, _ := smaller.Pa(p.AQL)
			if paA >= 1-p.Alpha-1e-9 {
				t.Errorf("case %d: c=%d also meets AQL, so c=%d not minimal", i, pl.C-1, pl.C)
			}
		}
	}
}

func TestSingleDesignNoFeasible(t *testing.T) {
	// AQL == LTPD is invalid; instead demand the impossible within a tiny cap.
	p := design.Params{AQL: 0.01, Alpha: 0.05, LTPD: 0.011, Beta: 0.001}
	_, err := design.Single(p, design.Limits{MaxSingleN: 5})
	if !errors.Is(err, design.ErrNoFeasiblePlan) {
		t.Fatalf("want ErrNoFeasiblePlan, got %v", err)
	}
}

func TestSingleDesignValidation(t *testing.T) {
	bad := []design.Params{
		{AQL: 0.1, Alpha: 0.05, LTPD: 0.1, Beta: 0.1},  // aql == ltpd
		{AQL: -0.1, Alpha: 0.05, LTPD: 0.1, Beta: 0.1}, // bad aql
		{AQL: 0.01, Alpha: 0, LTPD: 0.1, Beta: 0.1},    // alpha boundary
		{AQL: 0.01, Alpha: 0.05, LTPD: 0.1, Beta: 1},   // beta boundary
	}
	for i, p := range bad {
		if _, err := design.Single(p, design.Limits{}); err == nil {
			t.Errorf("bad params #%d accepted", i)
		}
	}
}

func TestDoubleDesign(t *testing.T) {
	p := design.Params{AQL: 0.01, Alpha: 0.05, LTPD: 0.08, Beta: 0.10}
	pl, err := design.Double(p, design.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if pl.Kind != sampling.Double {
		t.Fatal("not a double plan")
	}
	if pl.N2 != pl.N1 && pl.N2 != 2*pl.N1 {
		t.Errorf("n2=%d must equal n1=%d or 2*n1", pl.N2, pl.N1)
	}
	if !(pl.C1 < pl.R1 && pl.R1 <= pl.C2+1) {
		t.Errorf("linking rule violated: c1=%d r1=%d c2=%d", pl.C1, pl.R1, pl.C2)
	}
	okA, okL := meets(pl, p)
	if !okA || !okL {
		t.Errorf("double plan n1=%d n2=%d c1=%d c2=%d fails constraints (%v,%v)",
			pl.N1, pl.N2, pl.C1, pl.C2, okA, okL)
	}
	asn, _, err := pl.ASN(p.AQL)
	if err != nil {
		t.Fatal(err)
	}
	if asn < float64(pl.N1)-1e-9 || asn > float64(pl.N1+pl.N2)+1e-9 {
		t.Errorf("ASN %.3f outside range", asn)
	}
}

func TestDoubleDesignNoFeasibleTightCap(t *testing.T) {
	p := design.Params{AQL: 0.01, Alpha: 0.05, LTPD: 0.02, Beta: 0.01}
	_, err := design.Double(p, design.Limits{MaxDoubleN1: 3})
	if !errors.Is(err, design.ErrNoFeasiblePlan) {
		t.Fatalf("want ErrNoFeasiblePlan, got %v", err)
	}
}
