package sampling_test

import (
	"math"
	"testing"

	"qcinspect/internal/sampling"
)

func pa(t *testing.T, pl sampling.Plan, p float64) float64 {
	t.Helper()
	v, _, err := pl.Pa(p)
	if err != nil {
		t.Fatalf("Pa(%v): %v", p, err)
	}
	return v
}

// n=80, c=2 hand-checkable example.
func TestN80C2(t *testing.T) {
	pl := sampling.Plan{Kind: sampling.Single, N: 80, C: 2}
	// p=0.02: binomial acceptance ~0.7773...
	got := pa(t, pl, 0.02)
	if math.Abs(got-0.7844188871) > 1e-6 {
		t.Errorf("Pa(0.02)=%.10f want ~0.7844", got)
	}
	// p=0
	if got := pa(t, pl, 0); got != 1 {
		t.Errorf("Pa(0)=%.12f want exactly 1", got)
	}
	// p=1, c<n
	if got := pa(t, pl, 1); got != 0 {
		t.Errorf("Pa(1)=%.12f want exactly 0", got)
	}
}

func TestAcceptanceExtremesSingle(t *testing.T) {
	pl := sampling.Plan{Kind: sampling.Single, N: 30, C: 1}
	if pa(t, pl, 0) != 1 {
		t.Error("p=0 Pa must be exactly 1")
	}
	if pa(t, pl, 1) != 0 {
		t.Error("p=1 with c<n Pa must be exactly 0")
	}
	// c == n-1 at p=1 rejects.
	pl2 := sampling.Plan{Kind: sampling.Single, N: 5, C: 4}
	if pa(t, pl2, 1) != 0 {
		t.Error("p=1 with c=n-1 Pa must be 0")
	}
}

func TestOCMonotoneInC(t *testing.T) {
	// Same n, increasing c: the whole Pa curve is nondecreasing.
	const n = 60
	ps := []float64{0.001, 0.02, 0.05, 0.1, 0.3, 0.8}
	prev := make([]float64, len(ps))
	for i := range prev {
		prev[i] = -1
	}
	for c := 0; c < n; c++ {
		pl := sampling.Plan{Kind: sampling.Single, N: n, C: c}
		for i, p := range ps {
			got := pa(t, pl, p)
			if got < prev[i]-1e-15 {
				t.Errorf("n=%d c=%d p=%v: Pa %.10f < previous c's %.10f", n, c, p, got, prev[i])
			}
			prev[i] = got
		}
	}
}

func TestOCDecreasesWithN(t *testing.T) {
	// Fixed c, increasing n: Pa strictly decreases at every 0<p<1.
	for _, p := range []float64{0.01, 0.05, 0.2, 0.5, 0.9} {
		prev := 2.0
		for n := 3; n <= 200; n += 17 {
			c := 2
			if c >= n {
				continue
			}
			pl := sampling.Plan{Kind: sampling.Single, N: n, C: c}
			got := pa(t, pl, p)
			if got >= prev {
				t.Errorf("p=%v n=%d: Pa %.12f should be < %.12f", p, n, got, prev)
			}
			prev = got
		}
	}
}

func TestHyperApproxBinomial(t *testing.T) {
	bin := sampling.Plan{Kind: sampling.Single, N: 50, C: 2}
	hyp := sampling.Plan{Kind: sampling.Single, N: 50, C: 2, LotSize: 100000}
	for _, p := range []float64{0.01, 0.05, 0.2} {
		a := pa(t, bin, p)
		b := pa(t, hyp, p)
		if math.Abs(a-b) > 1e-3 {
			t.Errorf("p=%v hyper %v vs binom %v", p, b, a)
		}
	}
}

func TestDoubleDegenerateEqualsSingle(t *testing.T) {
	// n2=0, c1=c2, r1=c1+1 makes a double plan identical to single (n1,c1).
	for _, spec := range []struct{ n, c int }{
		{20, 0}, {20, 1}, {50, 3}, {80, 2},
	} {
		single := sampling.Plan{Kind: sampling.Single, N: spec.n, C: spec.c}
		dbl := sampling.Plan{
			Kind: sampling.Double,
			N1:   spec.n, C1: spec.c, R1: spec.c + 1,
			N2: 0, C2: spec.c,
		}
		if err := dbl.Validate(); err != nil {
			t.Fatalf("degenerate double invalid: %v", err)
		}
		for _, p := range []float64{0, 0.01, 0.05, 0.2, 0.7, 1} {
			a := pa(t, single, p)
			b := pa(t, dbl, p)
			if a != b {
				t.Errorf("n=%d c=%d p=%v: double %.17g != single %.17g", spec.n, spec.c, p, b, a)
			}
		}
	}
}

func TestASNRange(t *testing.T) {
	pl := sampling.Plan{
		Kind: sampling.Double,
		N1:   50, C1: 1, R1: 4, N2: 50, C2: 4,
	}
	for _, p := range []float64{0, 0.001, 0.02, 0.05, 0.1, 0.5, 1} {
		asn, _, err := pl.ASN(p)
		if err != nil {
			t.Fatal(err)
		}
		if asn < 50-1e-9 || asn > 100+1e-9 {
			t.Errorf("p=%v ASN %.4f outside [n1, n1+n2]", p, asn)
		}
	}
	// Degenerate p=0 => always accept first sample => ASN == n1.
	if asn, _, _ := pl.ASN(0); asn != 50 {
		t.Errorf("ASN(0)=%.4f want n1", asn)
	}
	// p=1 => always reject first sample => ASN == n1.
	if asn, _, _ := pl.ASN(1); asn != 50 {
		t.Errorf("ASN(1)=%.4f want n1", asn)
	}
}

func TestATIRange(t *testing.T) {
	pl := sampling.Plan{
		Kind: sampling.Single, N: 30, C: 2, LotSize: 1000,
	}
	for _, p := range []float64{0.0, 0.001, 0.02, 0.05, 0.2, 0.9, 1.0} {
		_, ati, _, err := pl.AOQATI(p)
		if err != nil {
			t.Fatal(err)
		}
		if ati < 30-1e-9 || ati > 1000+1e-9 {
			t.Errorf("p=%v ATI %.4f outside [n, N]", p, ati)
		}
	}
}

func TestATIDoubleRange(t *testing.T) {
	pl := sampling.Plan{
		Kind: sampling.Double,
		N1:   40, C1: 1, R1: 3, N2: 40, C2: 3, LotSize: 2000,
	}
	for _, p := range []float64{0.0, 0.03, 0.2, 1.0} {
		_, ati, _, err := pl.AOQATI(p)
		if err != nil {
			t.Fatal(err)
		}
		if ati < 40-1e-9 || ati > 2000+1e-9 {
			t.Errorf("double p=%v ATI %.4f outside bounds", p, ati)
		}
	}
}

func TestAOQLIsMaximum(t *testing.T) {
	pl := sampling.Plan{
		Kind: sampling.Single, N: 50, C: 2, LotSize: 5000,
	}
	res, _, err := pl.AOQL(0)
	if err != nil {
		t.Fatal(err)
	}
	if res.AOQL <= 0 {
		t.Fatalf("AOQL must be positive, got %v", res.AOQL)
	}
	// Dense scan: no point exceeds AOQL.
	for i := 0; i <= 2000; i++ {
		p := float64(i) / 2000
		a, _, _, err := pl.AOQATI(p)
		if err != nil {
			t.Fatal(err)
		}
		if a > res.AOQL+1e-12 {
			t.Fatalf("AOQ(%.5f)=%.10f exceeds AOQL %.10f", p, a, res.AOQL)
		}
	}
	if res.P <= 0 || res.P >= 1 {
		t.Errorf("AOQL arg p must be in (0,1), got %v", res.P)
	}
}

func TestRisks(t *testing.T) {
	pl := sampling.Plan{Kind: sampling.Single, N: 80, C: 2}
	r, _, err := pl.RisksAt(0.01, 0.08)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(r.Alpha-(1-r.PaAtAQL)) > 1e-12 {
		t.Error("alpha consistency")
	}
	if r.Beta != r.PaAtLTPD {
		t.Error("beta consistency")
	}
}

func TestPoissonFlagAndWarning(t *testing.T) {
	pl := sampling.Plan{Kind: sampling.Single, N: 1000, C: 5, ForcePoisson: true}
	v, info, err := pl.Pa(0.01) // np = 10 > 5
	if err != nil {
		t.Fatal(err)
	}
	if !info.Approximate || info.Family != "poisson" {
		t.Error("response must mark poisson approximation")
	}
	if len(info.Warnings) == 0 {
		t.Error("np>5 must produce a warning")
	}
	if !(v > 0 && v < 1) {
		t.Error("poisson Pa out of range")
	}
	// Small np: no warning.
	_, info2, _ := pl.Pa(0.001)
	if len(info2.Warnings) != 0 {
		t.Error("np<=5 should not warn")
	}
}

func TestValidation(t *testing.T) {
	bad := []sampling.Plan{
		{Kind: sampling.Single, N: 0, C: 0},
		{Kind: sampling.Single, N: 5, C: -1},
		{Kind: sampling.Single, N: 5, C: 5},
		{Kind: sampling.Double, N1: 10, C1: 3, R1: 2, N2: 10, C2: 4},
		{Kind: sampling.Double, N1: 10, C1: 1, R1: 6, N2: 10, C2: 4}, // r1 > c2+1
		{Kind: sampling.Single, N: 10, C: 1, LotSize: 5},             // N < n
	}
	for i, pl := range bad {
		if err := pl.Validate(); err == nil {
			t.Errorf("bad plan #%d accepted: %+v", i, pl)
		}
	}
	if _, _, err := (sampling.Plan{Kind: sampling.Single, N: 10, C: 1}).Pa(1.5); err == nil {
		t.Error("p>1 must be rejected")
	}
	if _, _, err := (sampling.Plan{Kind: sampling.Single, N: 10, C: 1}).Pa(-0.1); err == nil {
		t.Error("p<0 must be rejected")
	}
}

func TestDoublePaConsistency(t *testing.T) {
	// Accept + reject must partition the outcome space.
	pl := sampling.Plan{
		Kind: sampling.Double,
		N1:   40, C1: 1, R1: 4, N2: 40, C2: 4,
	}
	for _, p := range []float64{0.0, 0.02, 0.05, 0.2, 1.0} {
		got := pa(t, pl, p)
		if got < 0 || got > 1 {
			t.Errorf("p=%v Pa out of [0,1]: %v", p, got)
		}
	}
}

func TestOCCurvePoints(t *testing.T) {
	pl := sampling.Plan{Kind: sampling.Single, N: 20, C: 1}
	pts, _, err := pl.OCCurve(0, 1, 500)
	if err != nil {
		t.Fatal(err)
	}
	if len(pts) != 500 {
		t.Fatalf("got %d points", len(pts))
	}
	if pts[0].Pa != 1 || pts[len(pts)-1].Pa != 0 {
		t.Error("OC endpoints wrong")
	}
	if _, _, err := pl.OCCurve(0, 1, 501); err == nil {
		t.Error("points>500 must be rejected")
	}
}

func TestLargeNNoOverflow(t *testing.T) {
	// n in the thousands must not overflow or silently become zero.
	pl := sampling.Plan{Kind: sampling.Single, N: 4000, C: 20}
	v := pa(t, pl, 0.005)
	if !(v > 0 && v < 1) {
		t.Fatalf("Pa for n=4000 out of range: %v", v)
	}
	v2 := pa(t, pl, 0.5)
	if v2 != 0 {
		t.Errorf("Pa(0.5) for tight plan should underflow to ~0, got %v", v2)
	}
}
