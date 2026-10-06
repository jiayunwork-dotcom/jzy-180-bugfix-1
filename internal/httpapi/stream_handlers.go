package httpapi

import (
	"time"

	"github.com/labstack/echo/v4"

	"qcinspect/internal/sampling"
	"qcinspect/internal/store"
)

type createStreamReq struct {
	Name               string `json:"name"`
	NormalPlanID       int64  `json:"normal_plan_id"`
	TightenedPlanID    int64  `json:"tightened_plan_id"`
	ReducedPlanID      int64  `json:"reduced_plan_id"`
	ProductionStable   bool   `json:"production_stable"`
	SupervisorApproval bool   `json:"supervisor_approval"`
}

func (s *Server) createStream(c echo.Context) error {
	var req createStreamReq
	if err := c.Bind(&req); err != nil {
		return badRequest("invalid JSON: " + err.Error())
	}
	if req.Name == "" {
		return sampling.FieldError{Field: "name", Message: "must not be empty"}
	}
	var v sampling.ValidationErrors
	if req.NormalPlanID <= 0 {
		v.Add("normal_plan_id", "must be a positive plan id")
	}
	if req.TightenedPlanID <= 0 {
		v.Add("tightened_plan_id", "must be a positive plan id")
	}
	if req.ReducedPlanID <= 0 {
		v.Add("reduced_plan_id", "must be a positive plan id")
	}
	if len(v) > 0 {
		return v
	}
	sr, err := s.st.CreateStream(c.Request().Context(), store.CreateStreamParams{
		Name:               req.Name,
		NormalPlanID:       req.NormalPlanID,
		TightenedPlanID:    req.TightenedPlanID,
		ReducedPlanID:      req.ReducedPlanID,
		ProductionStable:   req.ProductionStable,
		SupervisorApproval: req.SupervisorApproval,
	})
	if err != nil {
		return err
	}
	return c.JSON(201, streamResp(sr))
}

func streamResp(sr store.StreamRow) map[string]any {
	var st any
	_ = jsonUnmarshalBytes(sr.CurrentState, &st)
	return map[string]any{
		"id":                  sr.ID,
		"name":                sr.Name,
		"normal_plan_id":      sr.NormalPlanID,
		"tightened_plan_id":   sr.TightenedPlanID,
		"reduced_plan_id":     sr.ReducedPlanID,
		"production_stable":   sr.ProductionStable,
		"supervisor_approval": sr.SupervisorApproval,
		"state":               st,
		"version":             sr.Version,
	}
}

func (s *Server) listStreams(c echo.Context) error {
	rows, err := s.st.ListStreams(c.Request().Context())
	if err != nil {
		return err
	}
	out := make([]map[string]any, 0, len(rows))
	for _, sr := range rows {
		out = append(out, streamResp(sr))
	}
	return c.JSON(200, out)
}

func (s *Server) getStream(c echo.Context) error {
	id, err := idParam(c, "id")
	if err != nil {
		return err
	}
	d, err := s.st.GetStreamDetail(c.Request().Context(), id)
	if err != nil {
		return err
	}
	return c.JSON(200, map[string]any{
		"stream":              streamResp(d.Stream),
		"production_stable":   d.ProductionStable,
		"supervisor_approval": d.SupervisorApproval,
		"state":               d.State,
		"batches":             d.Batches,
	})
}

type patchStreamReq struct {
	ProductionStable   *bool `json:"production_stable"`
	SupervisorApproval *bool `json:"supervisor_approval"`
}

func (s *Server) patchStream(c echo.Context) error {
	id, err := idParam(c, "id")
	if err != nil {
		return err
	}
	var req patchStreamReq
	if err := c.Bind(&req); err != nil {
		return badRequest("invalid JSON: " + err.Error())
	}
	if req.ProductionStable == nil && req.SupervisorApproval == nil {
		return sampling.FieldError{
			Field:   "production_stable",
			Message: "at least one of production_stable/supervisor_approval must be set",
		}
	}
	// Read current values to pass full replacement into the store.
	d, err := s.st.GetStreamDetail(c.Request().Context(), id)
	if err != nil {
		return err
	}
	stable, approval := d.ProductionStable, d.SupervisorApproval
	if req.ProductionStable != nil {
		stable = *req.ProductionStable
	}
	if req.SupervisorApproval != nil {
		approval = *req.SupervisorApproval
	}
	mr, err := s.st.SetFlags(c.Request().Context(), id, stable, approval)
	if err != nil {
		return err
	}
	return c.JSON(200, mutationResp(mr))
}

type resumeReq struct {
	OccurredAt *time.Time `json:"occurred_at"`
}

func (s *Server) resumeStream(c echo.Context) error {
	id, err := idParam(c, "id")
	if err != nil {
		return err
	}
	var req resumeReq
	if err := c.Bind(&req); err != nil {
		return badRequest("invalid JSON: " + err.Error())
	}
	at := time.Now().UTC()
	if req.OccurredAt != nil {
		at = req.OccurredAt.UTC()
	}
	mr, err := s.st.Resume(c.Request().Context(), id, at)
	if err != nil {
		return err
	}
	return c.JSON(200, mutationResp(mr))
}

type batchReq struct {
	BatchNo     string    `json:"batch_no"`
	InspectedAt time.Time `json:"inspected_at"`
	D1          int       `json:"d1"`
	D2          *int      `json:"d2"`
}

func (r batchReq) toInput() (store.BatchInput, error) {
	if r.BatchNo == "" {
		return store.BatchInput{}, sampling.FieldError{Field: "batch_no", Message: "must not be empty"}
	}
	if r.InspectedAt.IsZero() {
		return store.BatchInput{}, sampling.FieldError{Field: "inspected_at", Message: "must be an RFC3339 timestamp"}
	}
	return store.BatchInput{
		BatchNo:     r.BatchNo,
		InspectedAt: r.InspectedAt.UTC(),
		D1:          r.D1,
		D2:          r.D2,
	}, nil
}

func (s *Server) addBatch(c echo.Context) error {
	id, err := idParam(c, "id")
	if err != nil {
		return err
	}
	var req batchReq
	if err := c.Bind(&req); err != nil {
		return badRequest("invalid JSON: " + err.Error())
	}
	in, err := req.toInput()
	if err != nil {
		return err
	}
	mr, err := s.st.AddBatch(c.Request().Context(), id, in)
	if err != nil {
		return err
	}
	return c.JSON(201, mutationResp(mr))
}

func (s *Server) updateBatch(c echo.Context) error {
	id, err := idParam(c, "id")
	if err != nil {
		return err
	}
	bid, err := idParam(c, "bid")
	if err != nil {
		return err
	}
	var req batchReq
	if err := c.Bind(&req); err != nil {
		return badRequest("invalid JSON: " + err.Error())
	}
	in, err := req.toInput()
	if err != nil {
		return err
	}
	mr, err := s.st.UpdateBatch(c.Request().Context(), id, bid, in)
	if err != nil {
		return err
	}
	return c.JSON(200, mutationResp(mr))
}

func (s *Server) deleteBatch(c echo.Context) error {
	id, err := idParam(c, "id")
	if err != nil {
		return err
	}
	bid, err := idParam(c, "bid")
	if err != nil {
		return err
	}
	mr, err := s.st.DeleteBatch(c.Request().Context(), id, bid)
	if err != nil {
		return err
	}
	return c.JSON(200, mutationResp(mr))
}

func mutationResp(mr store.MutationResult) map[string]any {
	return map[string]any{
		"state":   mr.State,
		"batches": mr.All,
	}
}
