package store

import (
	"encoding/json"
	"time"

	_ "embed"

	"qcinspect/internal/statemachine"
)

//go:embed schema.sql
var migrationSQL string

// PlanRow is a persisted sampling plan档.
type PlanRow struct {
	ID         int64           `json:"id"`
	Name       string          `json:"name"`
	Definition json.RawMessage `json:"definition"`
	CreatedAt  time.Time       `json:"created_at"`
	UpdatedAt  time.Time       `json:"updated_at"`
}

// StreamRow is a persisted inspection stream.
type StreamRow struct {
	ID              int64  `json:"id"`
	Name            string `json:"name"`
	NormalPlanID    int64  `json:"normal_plan_id"`
	TightenedPlanID int64  `json:"tightened_plan_id"`
	ReducedPlanID   int64  `json:"reduced_plan_id"`
	// Current control flags (in force after the latest flag event). These are
	// what PATCH reads and what the API reports.
	ProductionStable   bool `json:"production_stable"`
	SupervisorApproval bool `json:"supervisor_approval"`
	// Initial control flags: the values in force from the timeline start
	// (before the first batch). Full replay seeds from these; flag events on
	// the timeline update them along the way. They differ from the current
	// flags once a flag change has been recorded.
	InitialProductionStable   bool            `json:"initial_production_stable"`
	InitialSupervisorApproval bool            `json:"initial_supervisor_approval"`
	CurrentState              json.RawMessage `json:"current_state"`
	Version                   int64           `json:"version"`
	CreatedAt                 time.Time       `json:"created_at"`
}

// BatchRow is a persisted batch with its folded record.
type BatchRow struct {
	ID          int64           `json:"id"`
	StreamID    int64           `json:"stream_id"`
	BatchNo     string          `json:"batch_no"`
	InspectedAt time.Time       `json:"inspected_at"`
	D1          int             `json:"d1"`
	D2          *int            `json:"d2,omitempty"`
	Result      json.RawMessage `json:"result"`
}

// EventRow is an administrative event: kind "resume" (manual resume after
// suspension) or kind "flags" (a point-in-time control-flag change).
type EventRow struct {
	ID                 int64     `json:"id"`
	StreamID           int64     `json:"stream_id"`
	Kind               string    `json:"kind"`
	OccurredAt         time.Time `json:"occurred_at"`
	ProductionStable   *bool     `json:"production_stable,omitempty"`
	SupervisorApproval *bool     `json:"supervisor_approval,omitempty"`
}

// BatchRecord couples the stored batch with its timeline index (0-based
// across batches+events) and folded result.
type BatchRecord struct {
	Batch  BatchRow
	Result statemachine.BatchResult
}
