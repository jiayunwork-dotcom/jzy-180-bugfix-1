// Package design searches for sampling plans satisfying producer's and
// consumer's risk constraints.
//
// Single plans: find the minimum n (and, at that n, the minimum c) with
// Pa(AQL) >= 1-alpha and Pa(LTPD) <= beta.
//
// Double plans: n2 is restricted to n1 or 2*n1; among all feasible plans the
// one with minimum ASN at p=AQL is chosen.
//
// Search uses the binomial model (design happens before a concrete lot size
// exists) and bounded enumeration; when nothing within the configured caps
// works, ErrNoFeasiblePlan is returned.
package design

import (
	"errors"

	"qcinspect/internal/sampling"
)

// ErrNoFeasiblePlan means no plan within the search bounds meets both risk
// constraints.
var ErrNoFeasiblePlan = errors.New("no feasible plan within search bounds")

// Params are the two-point risk requirements.
type Params struct {
	AQL   float64
	Alpha float64 // producer's risk target, 0 < alpha < 1
	LTPD  float64
	Beta  float64 // consumer's risk target, 0 < beta < 1
}

// Limits cap the search. Zero values select defaults.
type Limits struct {
	MaxSingleN  int // default 5000
	MaxDoubleN1 int // default 300
}

func (l *Limits) fill() {
	if l.MaxSingleN <= 0 {
		l.MaxSingleN = 5000
	}
	if l.MaxDoubleN1 <= 0 {
		l.MaxDoubleN1 = 300
	}
}

func validate(p Params) error {
	var v sampling.ValidationErrors
	sampling.CheckProb(&v, "aql", p.AQL)
	sampling.CheckProb(&v, "ltpd", p.LTPD)
	if p.AQL >= p.LTPD {
		v.Add("aql", "must satisfy aql < ltpd")
	}
	sampling.CheckProbOpen(&v, "alpha", p.Alpha)
	sampling.CheckProbOpen(&v, "beta", p.Beta)
	if len(v) > 0 {
		return v
	}
	return nil
}

// Single finds the minimum-sample single plan.
func Single(p Params, lim Limits) (sampling.Plan, error) {
	lim.fill()
	if err := validate(p); err != nil {
		return sampling.Plan{}, err
	}
	paAt := func(n, c int, q float64) float64 {
		pa, _, err := sampling.Plan{Kind: sampling.Single, N: n, C: c}.Pa(q)
		if err != nil {
			panic(err) // all inputs are structurally valid inside search
		}
		return pa
	}
	paGoalA := 1 - p.Alpha
	for n := 1; n <= lim.MaxSingleN; n++ {
		// Pa is nondecreasing in c. Find the smallest c meeting the AQL
		// constraint; that same c gives the best (smallest) Pa at LTPD.
		lo, hi := 0, n-1
		// If even c=n-1 cannot pass AQL, this n is infeasible.
		if paAt(n, hi, p.AQL) < paGoalA {
			continue
		}
		for lo < hi {
			mid := (lo + hi) / 2
			if paAt(n, mid, p.AQL) >= paGoalA {
				hi = mid
			} else {
				lo = mid + 1
			}
		}
		c := lo
		if paAt(n, c, p.LTPD) <= p.Beta {
			return sampling.Plan{Kind: sampling.Single, N: n, C: c}, nil
		}
	}
	return sampling.Plan{}, ErrNoFeasiblePlan
}
