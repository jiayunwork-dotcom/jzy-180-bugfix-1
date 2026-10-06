package sampling

import (
	"fmt"
	"math"

	"qcinspect/internal/dist"
)

// DistributionInfo describes what a computation actually used.
type DistributionInfo struct {
	Family      string `json:"family"`
	Approximate bool   `json:"approximate"`
	// Warnings produced for this computation, e.g. Poisson np > 5.
	Warnings []string `json:"warnings,omitempty"`
}

func (pl Plan) familyAt(p float64) (dist.Distribution, DistributionInfo, error) {
	info := DistributionInfo{}
	switch {
	case pl.ForcePoisson:
		np := p * float64(pl.firstSample())
		if np > 5 {
			info.Warnings = append(info.Warnings,
				fmt.Sprintf("Poisson approximation with n*p=%.4g > 5; approximation may be inaccurate", np))
		}
		info.Family = "poisson"
		info.Approximate = true
		return dist.NewPoisson(np), info, nil
	case pl.LotSize > 0 && !pl.ForcePoisson:
		D := int(math.Round(p * float64(pl.LotSize)))
		if D < 0 {
			D = 0
		}
		if D > pl.LotSize {
			D = pl.LotSize
		}
		info.Family = "hypergeometric"
		return dist.NewHypergeometric(pl.LotSize, D, pl.firstSample()), info, nil
	default:
		info.Family = "binomial"
		return dist.Binomial{N: pl.firstSample(), P: p}, info, nil
	}
}

// distributionFor builds a distribution over a sample of size sampleN. For
// hypergeometric plans the lot population D is shared; the drawn sample size
// changes (conditional second sample uses n2 from a lot depleted by n1).
func (pl Plan) distributionFor(sampleN, depletedLot, depletedD int, p float64) (dist.Distribution, DistributionInfo, error) {
	info := DistributionInfo{}
	switch {
	case pl.ForcePoisson:
		np := p * float64(sampleN)
		info.Family = "poisson"
		info.Approximate = true
		if np > 5 {
			info.Warnings = append(info.Warnings,
				fmt.Sprintf("Poisson approximation with sample n*p=%.4g > 5; approximation may be inaccurate", np))
		}
		return dist.NewPoisson(np), info, nil
	case pl.LotSize > 0:
		Nrem := pl.LotSize - depletedLot
		Drem := int(math.Round(p*float64(pl.LotSize))) - depletedD
		if Drem < 0 {
			Drem = 0
		}
		if Drem > Nrem {
			Drem = Nrem
		}
		info.Family = "hypergeometric"
		return dist.NewHypergeometric(Nrem, Drem, sampleN), info, nil
	default:
		info.Family = "binomial"
		return dist.Binomial{N: sampleN, P: p}, info, nil
	}
}

// Pa returns the probability of acceptance at lot fraction p.
//
// Single: Pa = P(X <= c).
// Double: Pa = P(d1 <= c1) + sum_{d1=c1+1}^{r1-1} P(d1) P(d2 <= c2-d1 | d1).
func (pl Plan) Pa(p float64) (float64, DistributionInfo, error) {
	if err := pl.Validate(); err != nil {
		return 0, DistributionInfo{}, err
	}
	var v ValidationErrors
	CheckProb(&v, "p", p)
	if len(v) > 0 {
		return 0, DistributionInfo{}, v
	}
	return pl.pa(p)
}

func (pl Plan) pa(p float64) (float64, DistributionInfo, error) {
	if pl.Kind == Single {
		d, info, err := pl.familyAt(p)
		if err != nil {
			return 0, DistributionInfo{}, err
		}
		return dist.CDF(d, pl.C), info, nil
	}

	d1, info, err := pl.familyAt(p)
	if err != nil {
		return 0, DistributionInfo{}, err
	}
	// First sample acceptance.
	logAcc := dist.LogCDF(d1, pl.C1)
	// Continue region d1 in [c1+1, r1-1].
	logSum := math.Inf(-1)
	for k := pl.C1 + 1; k <= pl.R1-1; k++ {
		logPk := dist.LogPMF(d1, k)
		if math.IsInf(logPk, -1) {
			continue
		}
		d2, _, derr := pl.distributionFor(pl.N2, pl.N1, k, p)
		if derr != nil {
			return 0, DistributionInfo{}, derr
		}
		// Conditional acceptance P(d2 <= c2-k | d1=k).
		logCond := dist.LogCDF(d2, pl.C2-k)
		logSum = logAddExp(logSum, logPk+logCond)
	}
	return math.Exp(logAddExp(logAcc, logSum)), info, nil
}

// ASN returns the average sample number (per lot, before any rejected-lot
// screening) at fraction p. Single plans return n.
func (pl Plan) ASN(p float64) (float64, DistributionInfo, error) {
	if err := pl.Validate(); err != nil {
		return 0, DistributionInfo{}, err
	}
	var v ValidationErrors
	CheckProb(&v, "p", p)
	if len(v) > 0 {
		return 0, DistributionInfo{}, v
	}
	if pl.Kind == Single {
		_, info, err := pl.familyAt(p)
		return float64(pl.N), info, err
	}
	d1, info, err := pl.familyAt(p)
	if err != nil {
		return 0, DistributionInfo{}, err
	}
	// P(continue to second sample) = P(c1 < d1 < r1).
	// = P(d1 <= r1-1) - P(d1 <= c1), both in log domain.
	logLeR := dist.LogCDF(d1, pl.R1-1)
	logLeC := dist.LogCDF(d1, pl.C1)
	pCont := math.Exp(logLeR) - math.Exp(logLeC)
	if pCont < 0 {
		pCont = 0
	}
	return float64(pl.N1) + float64(pl.N2)*pCont, info, nil
}
