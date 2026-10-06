package dist

import "math"

// Poisson is the Poisson(lambda) approximation to a binomial count.
type Poisson struct {
	Lambda float64
	// Support is capped for summation; true Poisson has unbounded support but
	// tail mass beyond Support is negligible (< ~1e-18 relative).
	maxK int
}

// NewPoisson builds a Poisson with a summation support chosen from lambda.
func NewPoisson(lambda float64) Poisson {
	// Cover at least lambda + 12*sqrt(lambda), with a small floor.
	m := int(math.Ceil(lambda + 12*math.Sqrt(lambda) + 10))
	if m < 64 {
		m = 64
	}
	return Poisson{Lambda: lambda, maxK: m}
}

func (p Poisson) Name() string { return "poisson" }
func (p Poisson) Support() int { return p.maxK }

// LogPMF returns log P(X=k) = -lambda + k*log(lambda) - log(k!).
func (p Poisson) LogPMF(k int) float64 {
	if k < 0 {
		return math.Inf(-1)
	}
	if p.Lambda == 0 {
		if k == 0 {
			return 0
		}
		return math.Inf(-1)
	}
	return -p.Lambda + float64(k)*math.Log(p.Lambda) - lgamma(float64(k+1))
}

func (p Poisson) PMF(k int) float64 {
	if k < 0 {
		return 0
	}
	return math.Exp(p.LogPMF(k))
}

// LogCDF returns log P(X<=k) via the shorter-tail log recurrence.
func (p Poisson) LogCDF(k int) float64 {
	if k < 0 {
		return math.Inf(-1)
	}
	if k >= p.maxK {
		return 0 // remainder is negligible by construction of maxK
	}
	if p.Lambda == 0 {
		return 0
	}
	// Forward sum 0..k when below mean, otherwise survival subtraction.
	if float64(k) <= p.Lambda {
		logTerm := -p.Lambda
		sum := logTerm
		for i := 0; i < k; i++ {
			logTerm += math.Log(p.Lambda) - math.Log(float64(i+1))
			sum = logAddExp(sum, logTerm)
		}
		return sum
	}
	// Sum k+1..maxK, treat the tiny remainder beyond maxK as zero.
	logTerm := p.LogPMF(p.maxK)
	upper := logTerm
	for i := p.maxK; i > k+1; i-- {
		logTerm += math.Log(float64(i)) - math.Log(p.Lambda) // P(i-1)/P(i)
		upper = logAddExp(upper, logTerm)
	}
	return math.Log1p(-math.Exp(upper))
}

func (p Poisson) CDF(k int) float64 {
	if k < 0 {
		return 0
	}
	return math.Exp(p.LogCDF(k))
}
