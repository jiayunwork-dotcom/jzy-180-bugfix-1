package httpapi

import (
	"strconv"

	"github.com/labstack/echo/v4"

	"qcinspect/internal/sampling"
	"qcinspect/internal/store"
)

type planUpsertReq struct {
	Name       string        `json:"name"`
	Definition sampling.Plan `json:"definition"`
}

type planResp struct {
	ID         int64         `json:"id"`
	Name       string        `json:"name"`
	Definition sampling.Plan `json:"definition"`
	CreatedAt  string        `json:"created_at"`
	UpdatedAt  string        `json:"updated_at"`
}

func toPlanResp(pr store.PlanRow) planResp {
	var pl sampling.Plan
	_ = jsonUnmarshalBytes(pr.Definition, &pl)
	return planResp{
		ID:         pr.ID,
		Name:       pr.Name,
		Definition: pl,
		CreatedAt:  pr.CreatedAt.UTC().Format("2006-01-02T15:04:05Z07:00"),
		UpdatedAt:  pr.UpdatedAt.UTC().Format("2006-01-02T15:04:05Z07:00"),
	}
}

func (s *Server) createPlan(c echo.Context) error {
	var req planUpsertReq
	if err := c.Bind(&req); err != nil {
		return badRequest("invalid JSON: " + err.Error())
	}
	if req.Name == "" {
		return sampling.FieldError{Field: "name", Message: "must not be empty"}
	}
	if err := req.Definition.Validate(); err != nil {
		return err
	}
	pr, err := s.st.CreatePlan(c.Request().Context(), req.Name, req.Definition)
	if err != nil {
		return err
	}
	return c.JSON(201, toPlanResp(pr))
}

func (s *Server) listPlans(c echo.Context) error {
	rows, err := s.st.ListPlans(c.Request().Context())
	if err != nil {
		return err
	}
	out := make([]planResp, 0, len(rows))
	for _, pr := range rows {
		out = append(out, toPlanResp(pr))
	}
	return c.JSON(200, out)
}

func (s *Server) getPlan(c echo.Context) error {
	id, err := idParam(c, "id")
	if err != nil {
		return err
	}
	pr, err := s.st.GetPlan(c.Request().Context(), id)
	if err != nil {
		return err
	}
	return c.JSON(200, toPlanResp(pr))
}

func (s *Server) getPlanByName(c echo.Context) error {
	name := c.QueryParam("name")
	if name == "" {
		return sampling.FieldError{Field: "name", Message: "query parameter name is required"}
	}
	pr, err := s.st.GetPlanByName(c.Request().Context(), name)
	if err != nil {
		return err
	}
	return c.JSON(200, toPlanResp(pr))
}

func (s *Server) updatePlan(c echo.Context) error {
	id, err := idParam(c, "id")
	if err != nil {
		return err
	}
	var req planUpsertReq
	if err := c.Bind(&req); err != nil {
		return badRequest("invalid JSON: " + err.Error())
	}
	if req.Name != "" {
		return sampling.FieldError{Field: "name", Message: "name cannot be changed; create a new plan instead"}
	}
	if err := req.Definition.Validate(); err != nil {
		return err
	}
	pr, replayed, err := s.st.UpdatePlanAndReplay(c.Request().Context(), id, req.Definition)
	if err != nil {
		return err
	}
	return c.JSON(200, map[string]any{
		"plan":             toPlanResp(pr),
		"replayed_streams": replayed,
	})
}

func (s *Server) deletePlan(c echo.Context) error {
	id, err := idParam(c, "id")
	if err != nil {
		return err
	}
	if err := s.st.DeletePlan(c.Request().Context(), id); err != nil {
		return err
	}
	return c.NoContent(204)
}

func idParam(c echo.Context, name string) (int64, error) {
	raw := c.Param(name)
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id <= 0 {
		return 0, sampling.FieldError{Field: name, Message: "must be a positive integer id"}
	}
	return id, nil
}
