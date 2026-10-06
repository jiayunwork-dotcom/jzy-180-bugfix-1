// Package statemachine implements the GB/T 2828.1 switching rules as a pure
// fold: (state, batch) -> (state', per-batch result). There is no hidden
// mutable context, which is what makes full replay and incremental replay
// from a checkpoint bit-for-bit identical.
//
// Severity transitions always take effect AFTER the triggering batch, so the
// triggering batch is still judged at its current severity:
//
//   - normal     -> tightened : >=2 lots not accepted among <=5 consecutive
//     lots on normal inspection
//   - tightened  -> normal    : 5 consecutive lots accepted on tightened
//   - normal     -> reduced   : switching score reaches 30 with the stream's
//     "production stable" and "supervisor approval"
//     flags both set
//   - reduced    -> normal    : one lot not accepted, or the stable flag
//     is revoked
//   - tightened  -> suspended : 5 lots (cumulative) not accepted during the
//     current tightened-inspection period
//   - suspended -(manual resume)-> tightened, counters restarted
//
// The switching score (GB/T 2828.1 switching-score procedure, operational form
// for user-defined plans) on a lot accepted under normal inspection:
//
//	single  : d=0 -> +3, d=1 -> +2, 2 <= d <= c (c>=2) -> +1
//	double  : accepted on the first sample scores like single on d1;
//	          accepted after the second sample scores +1
//	not accepted (or leaving normal inspection) resets the score to 0.
package statemachine

import (
	"qcinspect/internal/sampling"
)

// Severity is the current inspection strictness.
type Severity string

const (
	Normal    Severity = "normal"
	Tightened Severity = "tightened"
	Reduced   Severity = "reduced"
	Suspended Severity = "suspended"
)

// Transition labels the switching rule that fired after a batch.
type Transition string

const (
	TransNone        Transition = "none"
	TransToTightened Transition = "to_tightened"
	TransToNormal    Transition = "to_normal"
	TransToReduced   Transition = "to_reduced"
	TransSuspended   Transition = "to_suspended"
)

// PlanTriple are the three plan档 bound to a stream.
type PlanTriple struct {
	Normal    sampling.Plan
	Tightened sampling.Plan
	Reduced   sampling.Plan
}

// PlanRef identifies the plan used to judge a batch (snapshot for records).
type PlanRef struct {
	PlanID int64         `json:"plan_id"`
	Name   string        `json:"name"`
	Plan   sampling.Plan `json:"plan"`
}

// BatchInput is one chronologically ordered inspection result.
type BatchInput struct {
	ID          int64
	BatchNo     string
	InspectedAt string
	D1          int // nonconforming in the (first) sample
	D2          int // nonconforming in the second sample (double plans)
}

// BatchResult is the full per-batch record produced by the fold.
type BatchResult struct {
	Ordinal     int        `json:"ordinal"`
	Severity    Severity   `json:"severity"`
	Plan        PlanRef    `json:"plan"`
	Accepted    bool       `json:"accepted"`
	SecondTaken bool       `json:"second_sample_taken"`
	Score       int        `json:"switching_score"`
	Transition  Transition `json:"transition"`
}

// State is the complete folded state. Every counter that can influence a
// future decision lives here, so a serialized State is a valid checkpoint.
type State struct {
	Severity Severity `json:"severity"`
	// Switching score within the current normal-inspection run.
	Score int `json:"score"`
	// Acceptance outcomes of the current normal run, newest last; only the
	// final 5 are retained (the rule looks at <=5 consecutive lots).
	NormalWindow []bool `json:"normal_window"`
	// Consecutive accepted lots in the current tightened run.
	TightenedAcceptRun int `json:"tightened_accept_run"`
	// Cumulative (not necessarily consecutive) rejected lots since the
	// current tightened-inspection period began.
	TightenedRejectTotal int `json:"tightened_reject_total"`
	// Number of batches folded so far.
	Ordinal int `json:"ordinal"`
}

// InitialState is the state of a brand-new stream: normal inspection.
func InitialState() State {
	return State{Severity: Normal, NormalWindow: []bool{}}
}

// Flags are stream-wide control inputs constant during one replay.
type Flags struct {
	ProductionStable   bool
	SupervisorApproval bool
}

// FlagsExported is the replay-facing form of the control flags.
type FlagsExported struct {
	ProductionStable   bool
	SupervisorApproval bool
}

// ResumeAsTightened applies a manual resume after suspension: inspection
// restarts on tightened inspection with every switching counter reset.
func ResumeAsTightened(st State) State {
	st.Severity = Tightened
	st.Score = 0
	st.NormalWindow = []bool{}
	st.TightenedAcceptRun = 0
	st.TightenedRejectTotal = 0
	return st
}
