package dist

import "math"

// Binomial is Bin(n, p).
type Binomial struct {
	N int
	P float64
}

func (b Binomial) Name() string { return "binomial" }
func (b Binomial) Support() int { return b.N }

// LogPMF returns log P(X=k).
func (b Binomial) LogPMF(k int) float64 {
	if k < 0 || k > b.N {
		return math.Inf(-1)
	}
	n := float64(b.N)
	x := float64(k)
	return LogBinomial(b.N, k) + x*math.Log(b.P) + (n-x)*math.Log1p(-b.P)
}

func (b Binomial) PMF(k int) float64 {
	if k < 0 || k > b.N {
		return 0
	}
	return math.Exp(b.LogPMF(k))
}

// LogCDF returns log P(X<=k). Degenerate p == 0 / p == 1 are handled
// exactly, otherwise masses are summed on the shorter tail using a stable
// log-domain ratio recurrence anchored at an endpoint.
func (b Binomial) LogCDF(k int) float64 {
	if k < 0 {
		return math.Inf(-1)
	}
	if k >= b.N {
		return 0
	}
	if b.P == 0 {
		return 0 // X == 0 always
	}
	if b.P == 1 {
		if k < b.N {
			return math.Inf(-1) // X == n always
		}
		return 0
	}
	if k <= b.N-k {
		// Sum 0..k forward, anchor at i=0.
		logTerm := float64(b.N) * math.Log1p(-b.P) // log P(X=0)
		sum := logTerm
		for i := 0; i < k; i++ {
			// P(i+1)/P(i) = (n-i)/(i+1) * p/(1-p)
			logTerm += math.Log(float64(b.N-i)/float64(i+1)) +
				math.Log(b.P) - math.Log1p(-b.P)
			sum = logAddExp(sum, logTerm)
		}
		return sum
	}
	// Sum upper tail k+1..n backwards, anchor log P(X=n), then log1p.
	logTerm := float64(b.N) * math.Log(b.P) // log P(X=n)
	upper := logTerm
	// ratio P(i-1)/P(i) = i/(n-i+1) * (1-p)/p
	for i := b.N; i > k+1; i-- {
		logTerm += math.Log(float64(i)/float64(b.N-i+1)) +
			math.Log1p(-b.P) - math.Log(b.P)
		upper = logAddExp(upper, logTerm)
	}
	// P(X<=k) = 1 - P(X>k); exp(upper) is strictly < 1 here.
	return math.Log1p(-math.Exp(upper))
}

func (b Binomial) CDF(k int) float64 {
	if k < 0 {
		return 0
	}
	if k >= b.N {
		return 1
	}
	return math.Exp(b.LogCDF(k))
}
