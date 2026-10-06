package sampling

import (
	"math"
)

// OCPoint is one point of the operating characteristic curve.
type OCPoint struct {
	P   float64 `json:"p"`
	Pa  float64 `json:"pa"`
	ASN float64 `json:"asn,omitempty"` // populated for double plans
}

// OCCurve evaluates Pa (and ASN for double plans) at points points evenly
// spaced over [pMin, pMax]. points must be in [2, 500].
func (pl Plan) OCCurve(pMin, pMax float64, points int) ([]OCPoint, DistributionInfo, error) {
	if err := pl.Validate(); err != nil {
		return nil, DistributionInfo{}, err
	}
	var v ValidationErrors
	CheckProb(&v, "p_min", pMin)
	CheckProb(&v, "p_max", pMax)
	if pMin > pMax {
		v.add("p_min", "must be <= p_max")
	}
	if points < 2 || points > 500 {
		v.add("points", "must be within [2, 500]")
	}
	if len(v) > 0 {
		return nil, DistributionInfo{}, v
	}

	out := make([]OCPoint, 0, points)
	var mergedInfo DistributionInfo
	for i := 0; i < points; i++ {
		p := pMin
		if points > 1 {
			p = pMin + (pMax-pMin)*float64(i)/float64(points-1)
		}
		pa, info, err := pl.pa(p)
		if err != nil {
			return nil, DistributionInfo{}, err
		}
		mergeInfo(&mergedInfo, info)
		pt := OCPoint{P: p, Pa: pa}
		if pl.Kind == Double {
			asn, info2, err := pl.ASN(p)
			if err != nil {
				return nil, DistributionInfo{}, err
			}
			pt.ASN = asn
			mergeInfo(&mergedInfo, info2)
		} else {
			pt.ASN = float64(pl.N)
		}
		out = append(out, pt)
	}
	return out, mergedInfo, nil
}

func mergeInfo(dst *DistributionInfo, src DistributionInfo) {
	if dst.Family == "" {
		dst.Family = src.Family
	}
	if src.Approximate {
		dst.Approximate = true
	}
	dst.Warnings = append(dst.Warnings, src.Warnings...)
}

// Risks holds producer's and consumer's risks for given quality levels.
type Risks struct {
	AQL      float64 `json:"aql"`
	LTPD     float64 `json:"ltpd"`
	PaAtAQL  float64 `json:"pa_at_aql"`
	PaAtLTPD float64 `json:"pa_at_ltpd"`
	Alpha    float64 `json:"alpha"` // producer's risk: 1 - Pa(AQL)
	Beta     float64 `json:"beta"`  // consumer's risk: Pa(LTPD)
}

// RisksAt computes alpha and beta. LTPD must be strictly greater than AQL.
func (pl Plan) RisksAt(aql, ltpd float64) (Risks, DistributionInfo, error) {
	if err := pl.Validate(); err != nil {
		return Risks{}, DistributionInfo{}, err
	}
	var v ValidationErrors
	CheckProb(&v, "aql", aql)
	CheckProb(&v, "ltpd", ltpd)
	if aql >= ltpd {
		v.add("aql", "must satisfy aql < ltpd")
	}
	if len(v) > 0 {
		return Risks{}, DistributionInfo{}, v
	}
	paA, iA, err := pl.pa(aql)
	if err != nil {
		return Risks{}, DistributionInfo{}, err
	}
	paL, iL, err := pl.pa(ltpd)
	if err != nil {
		return Risks{}, DistributionInfo{}, err
	}
	var info DistributionInfo
	mergeInfo(&info, iA)
	mergeInfo(&info, iL)
	return Risks{
		AQL:      aql,
		LTPD:     ltpd,
		PaAtAQL:  paA,
		PaAtLTPD: paL,
		Alpha:    1 - paA,
		Beta:     paL,
	}, info, nil
}

// AOQPoint is average outgoing quality at one fraction p under the
// rejected lots are 100% inspected convention.
type AOQPoint struct {
	P   float64 `json:"p"`
	AOQ float64 `json:"aoq"`
	ATI float64 `json:"ati"`
}

// AOQATI computes average outgoing quality and average total inspection.
// Requires a lot size N (hypergeometric or binomial lot model):
//
//	AOQ = Pa * p * (N - ASN) / N
//	ATI = ASN + (1 - Pa) * (N - n_total)
//
// where n_total is n (single) or n1+n2 (double).
func (pl Plan) AOQATI(p float64) (aoq, ati float64, info DistributionInfo, err error) {
	if err = pl.Validate(); err != nil {
		return 0, 0, DistributionInfo{}, err
	}
	if pl.LotSize <= 0 {
		return 0, 0, DistributionInfo{}, FieldError{
			Field:   "lot_size",
			Message: "AOQ/ATI require a lot size N",
		}
	}
	var v ValidationErrors
	CheckProb(&v, "p", p)
	if len(v) > 0 {
		return 0, 0, DistributionInfo{}, v
	}
	pa, iPa, err := pl.pa(p)
	if err != nil {
		return 0, 0, DistributionInfo{}, err
	}
	asn, iASN, err := pl.ASN(p)
	if err != nil {
		return 0, 0, DistributionInfo{}, err
	}
	mergeInfo(&info, iPa)
	mergeInfo(&info, iASN)
	nTotal := float64(pl.sampleTotal())
	N := float64(pl.LotSize)
	aoq = pa * p * (N - asn) / N
	ati = asn + (1-pa)*(N-nTotal)
	return aoq, ati, info, nil
}

// AOQLResult is the maximum of the AOQ curve over p in (0,1).
type AOQLResult struct {
	AOQL float64 `json:"aoql"`
	P    float64 `json:"p"` // fraction at which the maximum is attained
	ATI  float64 `json:"ati_at_p"`
}

// AOQL scans the AOQ curve and refines the maximum with golden-section
// search. scanPoints is the density of the initial scan (capped internally).
func (pl Plan) AOQL(scanPoints int) (AOQLResult, DistributionInfo, error) {
	if scanPoints <= 0 {
		scanPoints = 400
	}
	if scanPoints > 5000 {
		scanPoints = 5000
	}
	if _, _, _, err := pl.AOQATI(0.5); err != nil {
		return AOQLResult{}, DistributionInfo{}, err
	}
	var info DistributionInfo
	bestP, bestAOQ, bestATI := 0.0, 0.0, 0.0
	type scanPt struct{ p, aoq, ati float64 }
	pts := make([]scanPt, 0, scanPoints+1)
	for i := 0; i <= scanPoints; i++ {
		p := float64(i) / float64(scanPoints)
		a, t, inf, err := pl.AOQATI(p)
		if err != nil {
			return AOQLResult{}, DistributionInfo{}, err
		}
		mergeInfo(&info, inf)
		pts = append(pts, scanPt{p, a, t})
		if a > bestAOQ {
			bestAOQ, bestP, bestATI = a, p, t
		}
	}
	// Refine within the bracketing interval around the discrete maximum.
	idx := 0
	for i, pt := range pts {
		if pt.p == bestP {
			idx = i
			break
		}
	}
	lo, hi := 0.0, 1.0
	if idx > 0 {
		lo = pts[idx-1].p
	}
	if idx < len(pts)-1 {
		hi = pts[idx+1].p
	}
	// Golden-section maximization (AOQ is unimodal: 0 at p=0 and p=1).
	gr := (math.Sqrt(5) - 1) / 2
	c := hi - gr*(hi-lo)
	d := lo + gr*(hi-lo)
	fc, _, _, _ := pl.AOQATI(c)
	fd, _, _, _ := pl.AOQATI(d)
	for i := 0; i < 60 && hi-lo > 1e-12; i++ {
		if fc > fd {
			hi = d
			d = c
			fd = fc
			c = hi - gr*(hi-lo)
			fc, _, _, _ = pl.AOQATI(c)
		} else {
			lo = c
			c = d
			fc = fd
			d = lo + gr*(hi-lo)
			fd, _, _, _ = pl.AOQATI(d)
		}
	}
	mid := (lo + hi) / 2
	if a, t, inf, err := pl.AOQATI(mid); err == nil && a >= bestAOQ {
		mergeInfo(&info, inf)
		bestAOQ, bestP, bestATI = a, mid, t
	}
	return AOQLResult{AOQL: bestAOQ, P: bestP, ATI: bestATI}, info, nil
}
