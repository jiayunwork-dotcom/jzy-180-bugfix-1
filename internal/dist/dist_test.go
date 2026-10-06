package dist_test

import (
	"math"
	"testing"

	"qcinspect/internal/dist"
)

func TestLogBinomialKnown(t *testing.T) {
	cases := []struct {
		n, k int
		want float64
	}{
		{10, 0, 0}, {10, 10, 0},
		{10, 1, math.Log(10)},
		{10, 3, math.Log(120)},
		{5, 2, math.Log(10)},
		{100, 50, math.Log(100891344545564193334812497256)}, // C(100,50)
	}
	for _, c := range cases {
		got := dist.LogBinomial(c.n, c.k)
		if math.Abs(got-c.want) > 1e-10 {
			t.Errorf("LogBinomial(%d,%d)=%.6f want %.6f", c.n, c.k, got, c.want)
		}
	}
	// Symmetry.
	if dist.LogBinomial(123, 44) != dist.LogBinomial(123, 79) {
		t.Error("LogBinomial must be symmetric in k and n-k")
	}
	// Large n: finite and consistent, never -Inf from underflow.
	v := dist.LogBinomial(5000, 100)
	if math.IsInf(v, -1) || math.IsInf(v, 1) {
		t.Errorf("LogBinomial(5000,100) is infinite: %v", v)
	}
	// Out of range.
	if !math.IsInf(dist.LogBinomial(5, 6), -1) {
		t.Error("k>n must be -Inf")
	}
}

func TestBinomialCDF(t *testing.T) {
	b := dist.Binomial{N: 80, P: 0.02}
	// P(X<=2) for n=80, p=0.02 (hand-checkable example).
	got := b.CDF(2)
	// Reference computed independently: sum of three terms via log domain.
	p0 := math.Exp(80 * math.Log(0.98))
	p1 := p0 * 80 * 0.02 / 0.98
	p2 := p1 * 79 / 2 * 0.02 / 0.98
	want := p0 + p1 + p2
	if math.Abs(got-want) > 1e-12 {
		t.Errorf("Bin(80,.02) CDF(2)=%.10f want %.10f", got, want)
	}
	if b.CDF(-1) != 0 {
		t.Error("CDF(-1) must be 0")
	}
	if b.CDF(80) != 1 {
		t.Error("CDF(n) must be 1")
	}
}

func TestBinomialExtremes(t *testing.T) {
	if dist.CDF(dist.Binomial{N: 50, P: 0}, 0) != 1 {
		t.Error("p=0: all good, Pa must be 1")
	}
	if dist.CDF(dist.Binomial{N: 50, P: 0}, 3) != 1 {
		t.Error("p=0: CDF at any k must be 1")
	}
	if dist.CDF(dist.Binomial{N: 50, P: 1}, 49) != 0 {
		t.Error("p=1, c<n: CDF must be exactly 0")
	}
	if dist.CDF(dist.Binomial{N: 50, P: 1}, 50) != 1 {
		t.Error("p=1, c==n: CDF must be 1")
	}
}

func TestHypergeometricMatchesBinomialForLargeN(t *testing.T) {
	// With n/N -> 0 the hypergeometric CDF approaches the binomial CDF.
	n, p, c := 20, 0.1, 2
	for _, N := range []int{200, 2000, 20000} {
		D := int(math.Round(p * float64(N)))
		h := dist.NewHypergeometric(N, D, n)
		b := dist.Binomial{N: n, P: float64(D) / float64(N)}
		gh, gb := h.CDF(c), b.CDF(c)
		diff := math.Abs(gh - gb)
		t.Logf("N=%d hyper=%.8f binom=%.8f diff=%.2e", N, gh, gb, diff)
		if N == 20000 && diff > 1e-4 {
			t.Errorf("large N: hyper %v too far from binom %v", gh, gb)
		}
	}
}

func TestHypergeometricFiniteTotal(t *testing.T) {
	// CDF over the full support must sum to 1 exactly (within eps).
	// N=100, D=30, n=40 -> lo = n-(N-D) = -30 -> 0, hi = min(n,D)=30.
	h := dist.NewHypergeometric(100, 30, 40)
	lo, hi := 0, 30
	if got := h.CDF(hi); math.Abs(got-1) > 1e-12 {
		t.Errorf("hyper CDF over support = %.12f", got)
	}
	if got := h.CDF(lo - 1); got != 0 {
		t.Errorf("hyper CDF below support = %v", got)
	}
	// A case with a nonzero lower bound: N=100, D=90, n=40 -> lo=30.
	h2 := dist.NewHypergeometric(100, 90, 40)
	if got := h2.CDF(29); got != 0 {
		t.Errorf("hyper CDF below support (lo=30) = %v", got)
	}
	if got := h2.CDF(40); math.Abs(got-1) > 1e-12 {
		t.Errorf("hyper CDF at/above support = %.12f", got)
	}
}

func TestPoissonCDF(t *testing.T) {
	po := dist.NewPoisson(2.5)
	if po.CDF(0) <= 0 || po.CDF(0) >= 1 {
		t.Error("poisson CDF(0) must be in (0,1)")
	}
	if math.Abs(po.CDF(0)-math.Exp(-2.5)) > 1e-12 {
		t.Error("poisson CDF(0) must be exp(-lambda)")
	}
	if po.CDF(po.Support()) < 1-1e-12 {
		t.Error("poisson CDF over support must approach 1")
	}
}

func TestBinomialPMFSumsToOne(t *testing.T) {
	for _, np := range [][2]int{{5, 0}, {5, 1}, {30, 3}, {100, 50}, {200, 97}} {
		b := dist.Binomial{N: np[0], P: float64(np[1]) / 100}
		ls := math.Inf(-1)
		for k := 0; k <= np[0]; k++ {
			l := b.LogPMF(k)
			// log-sum-exp
			if math.IsInf(ls, -1) {
				ls = l
			} else if l > ls {
				ls = l + math.Log1p(math.Exp(ls-l))
			} else {
				ls = ls + math.Log1p(math.Exp(l-ls))
			}
		}
		if math.Abs(ls) > 1e-9 {
			t.Errorf("Bin(%d,%g) PMF masses sum to exp(%.2e)", np[0], b.P, ls)
		}
	}
}
