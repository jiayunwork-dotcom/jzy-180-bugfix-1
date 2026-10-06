package dist

import "math"

// Hypergeometric models drawing n items without replacement from a lot of N
// containing D nonconforming items: P(X=k) = C(D,k) C(N-D, n-k) / C(N,n).
type Hypergeometric struct {
	N int // lot size
	D int // number of nonconforming in the lot
	n int // sample size
}

// NewHypergeometric builds the distribution. D is derived from the lot
// fraction p by rounding (round(p*N)); callers pass the already rounded D so
// that the exact integer population is explicit.
func NewHypergeometric(N, D, n int) Hypergeometric {
	return Hypergeometric{N: N, D: D, n: n}
}

func (h Hypergeometric) Name() string { return "hypergeometric" }
func (h Hypergeometric) Support() int {
	if h.n < h.D {
		return h.n
	}
	return h.D
}

// LogPMF returns log P(X=k) entirely from log-gamma binomial coefficients.
func (h Hypergeometric) LogPMF(k int) float64 {
	lo, hi := h.rangeK()
	if k < lo || k > hi {
		return math.Inf(-1)
	}
	return LogBinomial(h.D, k) +
		LogBinomial(h.N-h.D, h.n-k) -
		LogBinomial(h.N, h.n)
}

func (h Hypergeometric) PMF(k int) float64 {
	lo, hi := h.rangeK()
	if k < lo || k > hi {
		return 0
	}
	return math.Exp(h.LogPMF(k))
}

func (h Hypergeometric) rangeK() (int, int) {
	lo := h.n - (h.N - h.D)
	if lo < 0 {
		lo = 0
	}
	hi := h.n
	if hi > h.D {
		hi = h.D
	}
	return lo, hi
}

// LogCDF sums masses on the shorter tail in the log domain using the
// ratio recurrence P(k+1)/P(k) = ((D-k)(n-k)) / ((k+1)(N-D-n+k+1)).
func (h Hypergeometric) LogCDF(k int) float64 {
	lo, hi := h.rangeK()
	if k < lo {
		return math.Inf(-1)
	}
	if k >= hi {
		return 0
	}
	if k-lo <= hi-k {
		logTerm := h.LogPMF(lo)
		sum := logTerm
		for i := lo; i < k; i++ {
			logTerm += math.Log(float64(h.D-i)) + math.Log(float64(h.n-i)) -
				math.Log(float64(i+1)) -
				math.Log(float64(h.N-h.D-h.n+i+1))
			sum = logAddExp(sum, logTerm)
		}
		return sum
	}
	// Upper tail k+1..hi, anchor at hi and walk down with inverse ratio.
	logTerm := h.LogPMF(hi)
	upper := logTerm
	for i := hi; i > k+1; i-- {
		// P(i-1)/P(i) = i (N-D-n+i) / ((D-i+1)(n-i+1))
		logTerm += math.Log(float64(i)) + math.Log(float64(h.N-h.D-h.n+i)) -
			math.Log(float64(h.D-i+1)) - math.Log(float64(h.n-i+1))
		upper = logAddExp(upper, logTerm)
	}
	return math.Log1p(-math.Exp(upper))
}

func (h Hypergeometric) CDF(k int) float64 {
	lo, _ := h.rangeK()
	if k < lo {
		return 0
	}
	if k >= h.Support() {
		// k>=hi covered; support == min(n,D) == hi.
		return 1
	}
	return math.Exp(h.LogCDF(k))
}
