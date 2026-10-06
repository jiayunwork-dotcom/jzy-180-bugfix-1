// Package dist provides binomial, hypergeometric and Poisson distributions.
//
// All probability masses and cumulative probabilities are accumulated in the
// log domain (log-gamma based binomial coefficients) so that sample sizes up
// to several thousand neither overflow (binomial coefficients explode) nor
// silently underflow to 0 (probabilities that vastly exceed float64 range on
// both ends are converted through LogSumExp).
package dist

import (
	"math"
)

// Distribution is a discrete distribution of the number of nonconforming
// items drawn in a sample. All returned masses are ordinary probabilities.
type Distribution interface {
	// PMF returns P(X = k).
	PMF(k int) float64
	// CDF returns P(X <= k), exactly 1 for k >= Support and exactly 0 for k < 0.
	CDF(k int) float64
	// Support returns the largest value with nonzero probability.
	Support() int
	// Name identifies the family, e.g. "binomial".
	Name() string
}

// LogPMF returns P(X = k) as a logarithm (-Inf when k is outside support).
func LogPMF(d Distribution, k int) float64 {
	switch t := d.(type) {
	case interface{ LogPMF(int) float64 }:
		return t.LogPMF(k)
	}
	p := d.PMF(k)
	if p == 0 {
		return math.Inf(-1)
	}
	return math.Log(p)
}

// LogCDF returns P(X <= k) as a logarithm, summing masses in the log domain.
// When d implements an efficient LogCDF it is used directly; otherwise the
// shorter tail is summed (CDF via 1-CDF for k past the midpoint).
func LogCDF(d Distribution, k int) float64 {
	switch t := d.(type) {
	case interface{ LogCDF(int) float64 }:
		return t.LogCDF(k)
	}
	if k < 0 {
		return math.Inf(-1)
	}
	s := d.Support()
	if k >= s {
		return 0
	}
	// Sum the shorter side for accuracy.
	if k < s-k {
		lse := math.Inf(-1)
		for i := 0; i <= k; i++ {
			lse = logAddExp(lse, LogPMF(d, i))
		}
		return lse
	}
	// P(X<=k) = 1 - P(X>k), both computed in log domain then combined.
	upper := math.Inf(-1)
	for i := k + 1; i <= s; i++ {
		upper = logAddExp(upper, LogPMF(d, i))
	}
	// log(1 - exp(upper)) ; upper < 0 since at least one lower mass exists.
	return math.Log1p(-math.Exp(upper))
}

// CDF is a convenience wrapper around LogCDF.
func CDF(d Distribution, k int) float64 {
	if k < 0 {
		return 0
	}
	if k >= d.Support() {
		return 1
	}
	return math.Exp(LogCDF(d, k))
}

// logAddExp computes log(exp(a)+exp(b)) without over/underflow.
func logAddExp(a, b float64) float64 {
	if math.IsInf(a, -1) {
		return b
	}
	if math.IsInf(b, -1) {
		return a
	}
	if a > b {
		return a + math.Log1p(math.Exp(b-a))
	}
	return b + math.Log1p(math.Exp(a-b))
}
