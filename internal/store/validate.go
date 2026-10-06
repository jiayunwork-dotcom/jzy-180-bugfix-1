package store

import (
	"context"
	"encoding/json"
	"fmt"

	"qcinspect/internal/sampling"
	"qcinspect/internal/statemachine"
)

func jsonUnmarshal(raw []byte, v any) error { return json.Unmarshal(raw, v) }

// validateBatchAgainstPlans checks the nonconforming counts against the sample
// sizes of ALL three bound plans: a backdated batch may be judged under any
// severity, so it must be representable under each plan.
func validateBatchAgainstPlans(ctx context.Context, tx pgxTx, sr StreamRow, in BatchInput) error {
	triple, _, err := boundPlans(ctx, tx, sr)
	if err != nil {
		return err
	}
	plans := []sampling.Plan{triple.Normal, triple.Tightened, triple.Reduced}
	hasDouble := false
	for _, pl := range plans {
		n := pl.N
		n2 := 0
		if pl.Kind == sampling.Double {
			hasDouble = true
			n = pl.N1
			n2 = pl.N2
		}
		if in.D1 < 0 || in.D1 > n {
			return sampling.FieldError{
				Field:   "d1",
				Message: fmt.Sprintf("must be within [0, %d]", n),
			}
		}
		if in.D2 != nil && (*in.D2 < 0 || *in.D2 > n2) {
			return sampling.FieldError{
				Field:   "d2",
				Message: fmt.Sprintf("must be within [0, %d]", n2),
			}
		}
	}
	// Double plans need an explicit second-sample count (0 when not taken) so
	// a missing field can never be silently judged as zero.
	if hasDouble && in.D2 == nil {
		return sampling.FieldError{
			Field:   "d2",
			Message: "required for double sampling plans (use 0 when the second sample is not taken)",
		}
	}
	return nil
}

// decodeState parses a stream's current state.
func decodeState(sr StreamRow) (statemachine.State, error) {
	var st statemachine.State
	if err := json.Unmarshal(sr.CurrentState, &st); err != nil {
		return statemachine.State{}, err
	}
	if st.NormalWindow == nil {
		st.NormalWindow = []bool{}
	}
	return st, nil
}
