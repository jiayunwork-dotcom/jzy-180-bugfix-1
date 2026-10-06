package dist

import "math"

// LogBinomial returns log(C(n, k)) computed from log-gamma values. This never
// overflows for n in the thousands and stays accurate even when C(n,k) itself
// is far beyond the float64 range. Out-of-range k gives -Inf.
func LogBinomial(n, k int) float64 {
	if k < 0 || k > n || n < 0 {
		return math.Inf(-1)
	}
	if k == 0 || k == n {
		return 0
	}
	// Use the smaller of k and n-k to reduce cancellation.
	if k > n-k {
		k = n - k
	}
	// log C(n,k) = logGamma(n+1) - logGamma(k+1) - logGamma(n-k+1)
	return lgamma(float64(n+1)) -
		lgamma(float64(k+1)) -
		lgamma(float64(n-k+1))
}

// lgamma wraps math.Lgamma, discarding the sign return (all arguments here
// are positive, so Gamma is positive).
func lgamma(x float64) float64 {
	v, _ := math.Lgamma(x)
	return v
}
