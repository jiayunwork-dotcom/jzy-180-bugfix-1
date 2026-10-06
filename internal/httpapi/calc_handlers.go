package httpapi

import (
	"github.com/labstack/echo/v4"

	"qcinspect/internal/design"
	"qcinspect/internal/sampling"
)

// planCarrier allows inline plan computation: { "plan": {...}, ... }
type planCarrier struct {
	PlanID *int64         `json:"plan_id,omitempty"`
	Plan   *sampling.Plan `json:"plan,omitempty"`
}

// resolvePlan returns the plan to evaluate: the stored plan when the path or
// body references an id, otherwise the inline definition.
func (s *Server) resolvePlan(c echo.Context, body planCarrier) (sampling.Plan, error) {
	if id := c.Param("id"); id != "" {
		pid, err := idParam(c, "id")
		if err != nil {
			return sampling.Plan{}, err
		}
		pr, err := s.st.GetPlan(c.Request().Context(), pid)
		if err != nil {
			return sampling.Plan{}, err
		}
		var pl sampling.Plan
		if err := jsonUnmarshalBytes(pr.Definition, &pl); err != nil {
			return sampling.Plan{}, err
		}
		return pl, nil
	}
	if body.PlanID != nil {
		pr, err := s.st.GetPlan(c.Request().Context(), *body.PlanID)
		if err != nil {
			return sampling.Plan{}, err
		}
		var pl sampling.Plan
		if err := jsonUnmarshalBytes(pr.Definition, &pl); err != nil {
			return sampling.Plan{}, err
		}
		return pl, nil
	}
	if body.Plan == nil {
		return sampling.Plan{}, sampling.FieldError{
			Field: "plan", Message: "either plan, plan_id or a path id is required",
		}
	}
	return *body.Plan, nil
}

type paReq struct {
	planCarrier
	P float64 `json:"p"`
}

func (s *Server) calcPA(c echo.Context) error {
	var req paReq
	if err := c.Bind(&req); err != nil {
		return badRequest("invalid JSON: " + err.Error())
	}
	pl, err := s.resolvePlan(c, req.planCarrier)
	if err != nil {
		return err
	}
	pa, info, err := pl.Pa(req.P)
	if err != nil {
		return err
	}
	asn, _, _ := pl.ASN(req.P)
	return c.JSON(200, map[string]any{
		"p": req.P, "pa": pa, "asn": asn, "distribution": info,
	})
}

type ocReq struct {
	planCarrier
	PMin   float64 `json:"p_min"`
	PMax   float64 `json:"p_max"`
	Points int     `json:"points"`
}

func (s *Server) calcOC(c echo.Context) error {
	var req ocReq
	if err := c.Bind(&req); err != nil {
		return badRequest("invalid JSON: " + err.Error())
	}
	pl, err := s.resolvePlan(c, req.planCarrier)
	if err != nil {
		return err
	}
	if req.Points == 0 {
		req.Points = 101
	}
	points, info, err := pl.OCCurve(req.PMin, req.PMax, req.Points)
	if err != nil {
		return err
	}
	return c.JSON(200, map[string]any{"points": points, "distribution": info})
}

type risksReq struct {
	planCarrier
	AQL  float64 `json:"aql"`
	LTPD float64 `json:"ltpd"`
}

func (s *Server) calcRisks(c echo.Context) error {
	var req risksReq
	if err := c.Bind(&req); err != nil {
		return badRequest("invalid JSON: " + err.Error())
	}
	pl, err := s.resolvePlan(c, req.planCarrier)
	if err != nil {
		return err
	}
	r, info, err := pl.RisksAt(req.AQL, req.LTPD)
	if err != nil {
		return err
	}
	return c.JSON(200, map[string]any{"risks": r, "distribution": info})
}

type aoqReq struct {
	planCarrier
	PMin   float64 `json:"p_min,omitempty"`
	PMax   float64 `json:"p_max,omitempty"`
	Points int     `json:"points,omitempty"`
	P      float64 `json:"p,omitempty"`
}

func (s *Server) calcAOQ(c echo.Context) error {
	var req aoqReq
	if err := c.Bind(&req); err != nil {
		return badRequest("invalid JSON: " + err.Error())
	}
	pl, err := s.resolvePlan(c, req.planCarrier)
	if err != nil {
		return err
	}
	// Single point when p is given.
	if req.Points == 0 && req.PMin == 0 && req.PMax == 0 {
		aoq, ati, info, err := pl.AOQATI(req.P)
		if err != nil {
			return err
		}
		return c.JSON(200, map[string]any{
			"p": req.P, "aoq": aoq, "ati": ati, "distribution": info,
		})
	}
	pMin, pMax, pts := req.PMin, req.PMax, req.Points
	if pts == 0 {
		pts = 101
	}
	var v sampling.ValidationErrors
	if pMin >= pMax {
		v.Add("p_min", "must be < p_max")
	}
	if pts < 2 || pts > 500 {
		v.Add("points", "must be within [2, 500]")
	}
	if len(v) > 0 {
		return v
	}
	type pt struct {
		P   float64 `json:"p"`
		AOQ float64 `json:"aoq"`
		ATI float64 `json:"ati"`
	}
	out := make([]pt, 0, pts)
	var infoAny sampling.DistributionInfo
	for i := 0; i < pts; i++ {
		p := pMin + (pMax-pMin)*float64(i)/float64(pts-1)
		a, t, info, err := pl.AOQATI(p)
		if err != nil {
			return err
		}
		infoAny = info
		out = append(out, pt{p, a, t})
	}
	return c.JSON(200, map[string]any{"points": out, "distribution": infoAny})
}

func (s *Server) calcAOQL(c echo.Context) error {
	var req planCarrier
	if err := c.Bind(&req); err != nil {
		return badRequest("invalid JSON: " + err.Error())
	}
	pl, err := s.resolvePlan(c, req)
	if err != nil {
		return err
	}
	res, info, err := pl.AOQL(0)
	if err != nil {
		return err
	}
	return c.JSON(200, map[string]any{"aoql": res, "distribution": info})
}

type designReq struct {
	AQL         float64 `json:"aql"`
	Alpha       float64 `json:"alpha"`
	LTPD        float64 `json:"ltpd"`
	Beta        float64 `json:"beta"`
	MaxSingleN  int     `json:"max_n,omitempty"`
	MaxDoubleN1 int     `json:"max_n1,omitempty"`
}

func (d designReq) params() design.Params {
	return design.Params{AQL: d.AQL, Alpha: d.Alpha, LTPD: d.LTPD, Beta: d.Beta}
}
func (d designReq) limits() design.Limits {
	return design.Limits{MaxSingleN: d.MaxSingleN, MaxDoubleN1: d.MaxDoubleN1}
}

func (s *Server) designSingle(c echo.Context) error {
	var req designReq
	if err := c.Bind(&req); err != nil {
		return badRequest("invalid JSON: " + err.Error())
	}
	pl, err := design.Single(req.params(), req.limits())
	if err != nil {
		return err
	}
	r, _, _ := pl.RisksAt(req.AQL, req.LTPD)
	return c.JSON(200, map[string]any{"plan": pl, "risks": r})
}

func (s *Server) designDouble(c echo.Context) error {
	var req designReq
	if err := c.Bind(&req); err != nil {
		return badRequest("invalid JSON: " + err.Error())
	}
	pl, err := design.Double(req.params(), req.limits())
	if err != nil {
		return err
	}
	r, _, _ := pl.RisksAt(req.AQL, req.LTPD)
	asn, _, _ := pl.ASN(req.AQL)
	return c.JSON(200, map[string]any{"plan": pl, "risks": r, "asn_at_aql": asn})
}
