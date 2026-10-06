// Package sampling implements single and double acceptance sampling plans:
// acceptance probability (OC curve), average sample number (ASN), average
// outgoing quality (AOQ/AOQL) and average total inspection (ATI).
package sampling

import (
	"fmt"
	"math"
)

// Family selects the distribution used to model nonconforming counts.
type Family string

const (
	FamilyAuto     Family = "" // N present -> hypergeometric, else binomial
	FamilyBinomial Family = "binomial"
	FamilyPoisson  Family = "poisson" // explicit approximation
)

// Kind distinguishes single and double sampling plans.
type Kind string

const (
	Single Kind = "single"
	Double Kind = "double"
)

// Plan is a sampling plan definition.
//
// Single: n, c used (reject on more than c).
// Double: first sample n1 with acceptance c1 / rejection r1
// (c1 < r1; c1 < d1 < r1 forces a second sample), second sample n2, combined
// acceptance c2 (reject combined count above c2), with r1 <= c2+1.
//
// LotSize N == 0 means the lot fraction is treated as a process fraction
// (binomial). ForcePoisson selects the Poisson approximation; responses then
// report Approximate=true.
type Plan struct {
	Kind         Kind   `json:"kind"`
	N            int    `json:"n,omitempty"`
	C            int    `json:"c,omitempty"`
	N1           int    `json:"n1,omitempty"`
	C1           int    `json:"c1,omitempty"`
	R1           int    `json:"r1,omitempty"`
	N2           int    `json:"n2,omitempty"`
	C2           int    `json:"c2,omitempty"`
	LotSize      int    `json:"lot_size,omitempty"`
	Family       Family `json:"family,omitempty"`
	ForcePoisson bool   `json:"force_poisson,omitempty"`
}

// FieldError points at the offending request field.
type FieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

func (e FieldError) Error() string { return fmt.Sprintf("%s: %s", e.Field, e.Message) }

// ValidationErrors is the ordered list of all field problems found.
type ValidationErrors []FieldError

func (v ValidationErrors) Error() string {
	if len(v) == 0 {
		return "validation error"
	}
	s := v[0].Error()
	for _, e := range v[1:] {
		s += "; " + e.Error()
	}
	return s
}

func (v *ValidationErrors) add(field, msg string) { *v = append(*v, FieldError{field, msg}) }

// CheckProb validates that p is within [0,1], recording a field error.
func CheckProb(verrs *ValidationErrors, field string, p float64) {
	if math.IsNaN(p) || p < 0 || p > 1 {
		verrs.add(field, "must be within [0,1]")
	}
}

// CheckProbOpen validates that p is strictly within (0,1).
func CheckProbOpen(verrs *ValidationErrors, field string, p float64) {
	if math.IsNaN(p) || p <= 0 || p >= 1 {
		verrs.add(field, "must be within (0,1)")
	}
}

// Add exposes the field-error collector for other packages.
func (v *ValidationErrors) Add(field, msg string) { v.add(field, msg) }

// Validate checks structural constraints (not the probability inputs, which
// are validated at each call site).
func (pl Plan) Validate() error {
	var v ValidationErrors
	switch pl.Kind {
	case Single:
		if pl.N < 1 {
			v.add("n", "must be >= 1")
		}
		if pl.C < 0 {
			v.add("c", "must be >= 0")
		}
		if pl.N >= 1 && pl.C >= pl.N {
			v.add("c", "must be < n")
		}
	case Double:
		if pl.N1 < 1 {
			v.add("n1", "must be >= 1")
		}
		if pl.N2 < 0 {
			v.add("n2", "must be >= 0")
		}
		if pl.C1 < 0 {
			v.add("c1", "must be >= 0")
		}
		if pl.R1 < 0 {
			v.add("r1", "must be >= 0")
		}
		if pl.C2 < 0 {
			v.add("c2", "must be >= 0")
		}
		if pl.C1 >= pl.R1 {
			v.add("c1", "must satisfy c1 < r1")
		}
		if pl.R1 > pl.C2+1 {
			v.add("r1", "must satisfy r1 <= c2+1")
		}
		if pl.C1 > pl.C2 {
			v.add("c1", "must be <= c2")
		}
		// Combined sample must be large enough for c2 to be attainable.
		if pl.N1 >= 1 && pl.N2 >= 0 && pl.C2 >= pl.N1+pl.N2 {
			v.add("c2", "must be < n1+n2")
		}
	default:
		v.add("kind", "must be 'single' or 'double'")
	}
	if pl.LotSize < 0 {
		v.add("lot_size", "must be >= 1 or omitted")
	}
	if pl.LotSize > 0 {
		total := pl.sampleTotal()
		if total > 0 && pl.LotSize < total {
			v.add("lot_size", "must be >= total sample size")
		}
	}
	if len(v) > 0 {
		return v
	}
	return nil
}

func (pl Plan) sampleTotal() int {
	if pl.Kind == Double {
		return pl.N1 + pl.N2
	}
	return pl.N
}

func (pl Plan) firstSample() int {
	if pl.Kind == Double {
		return pl.N1
	}
	return pl.N
}

// FirstSampleSize returns the first-sample size (n1 for double, n for single).
func (pl Plan) FirstSampleSize() int { return pl.firstSample() }
