package design

import (
	"math"

	"qcinspect/internal/dist"
	"qcinspect/internal/sampling"
)

// Double searches double plans with n2 = n1 or n2 = 2*n1 and the standard
// linking r1 = c2+1 (the GB/T 2828.1 double-sampling form). Among feasible
// plans it minimizes ASN at p=AQL; deterministic tie-breaks keep the result
// stable.
//
// For fixed (n1, n2, c2) and quality p, acceptance as a function of c1 is
//
//	Pa(c1) = F1(c1) + sum_{k=c1+1..c2} P1(k) F2(c2-k),
//
// monotone increasing in c1. We precompute P1 and the weighted terms once,
// turning every c1 probe into O(1); the largest c1 satisfying the LTPD
// constraint also minimizes the ASN at AQL (it shrinks the continue region),
// so one candidate per (n1, n2, c2) suffices.
func Double(p Params, lim Limits) (sampling.Plan, error) {
	lim.fill()
	if err := validate(p); err != nil {
		return sampling.Plan{}, err
	}
	paGoalA := 1 - p.Alpha

	var best sampling.Plan
	bestASN := math.Inf(1)
	found := false

	consider := func(n1, mult int) {
		n2 := mult * n1
		d1A := dist.Binomial{N: n1, P: p.AQL}
		d1L := dist.Binomial{N: n1, P: p.LTPD}
		d2A := dist.Binomial{N: n2, P: p.AQL}
		d2L := dist.Binomial{N: n2, P: p.LTPD}

		p1A := binomPMFTable(d1A)
		p1L := binomPMFTable(d1L)

		// c2 stays <= n1 so that r1 = c2+1 is reachable on the first sample.
		for c2 := 1; c2 <= n1; c2++ {
			// F2(j) tables up to c2.
			f2A := binomCDFTable(d2A, c2)
			f2L := binomCDFTable(d2L, c2)

			// pref1A[i] = F1(i); sufA/B weighted tail sums.
			prefA := prefixCDF(p1A)
			prefL := prefixCDF(p1L)
			// tailA[t] = sum_{k=t..c2} P1(k)*F2(c2-k)
			tailA := weightedTail(p1A, f2A, c2)
			tailL := weightedTail(p1L, f2L, c2)

			paA := func(c1 int) float64 { return prefA[c1] + tailA[c1+1] }
			paL := func(c1 int) float64 { return prefL[c1] + tailL[c1+1] }

			// Largest c1 in [0, c2-1] with Pa(LTPD) <= beta.
			lo, hi := 0, c2-1
			if paL(hi) > p.Beta {
				continue // even the tightest continue region leaks too much
			}
			for lo < hi {
				mid := (lo + hi + 1) / 2
				if paL(mid) <= p.Beta {
					lo = mid
				} else {
					hi = mid - 1
				}
			}
			c1 := lo
			if paA(c1) < paGoalA {
				continue
			}
			// ASN at AQL: n1 + n2 * P(c1 < d1 < c2+1)
			pCont := prefA[c2] - prefA[c1]
			asn := float64(n1) + float64(n2)*pCont

			better := asn < bestASN-1e-12 ||
				(math.Abs(asn-bestASN) <= 1e-12 &&
					(n1 < best.N1 ||
						(n1 == best.N1 && n2 < best.N2) ||
						(n1 == best.N1 && n2 == best.N2 && c2 < best.C2) ||
						(n1 == best.N1 && n2 == best.N2 && c2 == best.C2 && c1 < best.C1)))
			if !found || better {
				found = true
				bestASN = asn
				best = sampling.Plan{
					Kind: sampling.Double,
					N1:   n1, C1: c1, R1: c2 + 1,
					N2: n2, C2: c2,
				}
			}
		}
	}

	for n1 := 1; n1 <= lim.MaxDoubleN1; n1++ {
		consider(n1, 1)
		consider(n1, 2)
	}
	if !found {
		return sampling.Plan{}, ErrNoFeasiblePlan
	}
	return best, nil
}

// binomPMFTable returns P(X=k) for k=0..n using a forward recurrence.
func binomPMFTable(b dist.Binomial) []float64 {
	n := b.N
	out := make([]float64, n+1)
	if b.P == 0 {
		out[0] = 1
		return out
	}
	if b.P == 1 {
		out[n] = 1
		return out
	}
	out[0] = math.Exp(float64(n) * math.Log1p(-b.P))
	for k := 0; k < n; k++ {
		out[k+1] = out[k] * float64(n-k) / float64(k+1) * b.P / (1 - b.P)
	}
	return out
}

// binomCDFTable returns F(j)=P(X<=j) for j=0..maxJ.
func binomCDFTable(b dist.Binomial, maxJ int) []float64 {
	pmf := binomPMFTable(b)
	if maxJ > b.N {
		maxJ = b.N
	}
	out := make([]float64, maxJ+1)
	s := 0.0
	for j := 0; j <= maxJ; j++ {
		s += pmf[j]
		out[j] = s
	}
	return out
}

func prefixCDF(pmf []float64) []float64 {
	out := make([]float64, len(pmf))
	s := 0.0
	for i, v := range pmf {
		s += v
		out[i] = s
	}
	return out
}

// weightedTail returns tail[t] = sum_{k=t..c2} pmf1[k]*f2[c2-k] for t=0..c2+1.
func weightedTail(pmf1, f2 []float64, c2 int) []float64 {
	tail := make([]float64, c2+2)
	for k := c2; k >= 0; k-- {
		f := 0.0
		if c2-k < len(f2) {
			f = f2[c2-k]
		} else {
			f = 1
		}
		tail[k] = tail[k+1] + pmf1[k]*f
	}
	return tail
}
