// Package httpapi exposes the service over HTTP with Echo. JSON only; there
// is no frontend.
package httpapi

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v4"

	"qcinspect/internal/design"
	"qcinspect/internal/sampling"
	"qcinspect/internal/store"
)

// Server wires the store into HTTP handlers.
type Server struct {
	st *store.Store
}

// New builds the Echo instance with all routes registered.
func New(st *store.Store) *echo.Echo {
	srv := &Server{st: st}
	e := echo.New()
	e.HideBanner = true
	e.HTTPErrorHandler = errorHandler

	api := e.Group("/api")

	// Plans
	api.POST("/plans", srv.createPlan)
	api.GET("/plans", srv.listPlans)
	api.GET("/plans/by-name", srv.getPlanByName)
	api.GET("/plans/:id", srv.getPlan)
	api.PUT("/plans/:id", srv.updatePlan)
	api.DELETE("/plans/:id", srv.deletePlan)

	// Calculations (inline plan, or plan_id referencing a stored档)
	api.POST("/calculate/pa", srv.calcPA)
	api.POST("/calculate/oc", srv.calcOC)
	api.POST("/calculate/risks", srv.calcRisks)
	api.POST("/calculate/aoq", srv.calcAOQ)
	api.POST("/calculate/aoql", srv.calcAOQL)
	api.POST("/design/single", srv.designSingle)
	api.POST("/design/double", srv.designDouble)

	// Stored-plan calculation shortcuts
	api.POST("/plans/:id/pa", srv.calcPA)
	api.POST("/plans/:id/oc", srv.calcOC)
	api.POST("/plans/:id/risks", srv.calcRisks)
	api.POST("/plans/:id/aoq", srv.calcAOQ)
	api.POST("/plans/:id/aoql", srv.calcAOQL)

	// Streams
	api.POST("/streams", srv.createStream)
	api.GET("/streams", srv.listStreams)
	api.GET("/streams/:id", srv.getStream)
	api.PATCH("/streams/:id", srv.patchStream)
	api.POST("/streams/:id/resume", srv.resumeStream)

	// Batches
	api.POST("/streams/:id/batches", srv.addBatch)
	api.PUT("/streams/:id/batches/:bid", srv.updateBatch)
	api.DELETE("/streams/:id/batches/:bid", srv.deleteBatch)

	e.GET("/healthz", func(c echo.Context) error { return c.NoContent(http.StatusOK) })
	return e
}

// errBody is the standard error envelope.
type errBody struct {
	Error  string                `json:"error"`
	Fields []sampling.FieldError `json:"fields,omitempty"`
	Code   string                `json:"code,omitempty"`
}

func errorHandler(err error, c echo.Context) {
	if c.Response().Committed {
		return
	}
	var ve sampling.ValidationErrors
	if errors.As(err, &ve) {
		_ = c.JSON(http.StatusUnprocessableEntity, errBody{
			Error:  "validation failed",
			Fields: []sampling.FieldError(ve),
		})
		return
	}
	var fe sampling.FieldError
	if errors.As(err, &fe) {
		_ = c.JSON(http.StatusUnprocessableEntity, errBody{
			Error:  "validation failed",
			Fields: []sampling.FieldError{fe},
		})
		return
	}
	switch {
	case errors.Is(err, store.ErrNotFound):
		_ = c.JSON(http.StatusNotFound, errBody{Error: err.Error()})
	case errors.Is(err, store.ErrDuplicateName):
		_ = c.JSON(http.StatusConflict, errBody{Error: err.Error(), Code: "duplicate_name"})
	case errors.Is(err, store.ErrBoundPlan):
		_ = c.JSON(http.StatusConflict, errBody{Error: err.Error(), Code: "plan_in_use"})
	case errors.Is(err, store.ErrSuspended):
		_ = c.JSON(http.StatusConflict, errBody{Error: err.Error(), Code: "stream_suspended"})
	case errors.Is(err, design.ErrNoFeasiblePlan):
		_ = c.JSON(http.StatusUnprocessableEntity, errBody{
			Error: err.Error(), Code: "no_feasible_plan",
		})
	default:
		var he *echo.HTTPError
		if errors.As(err, &he) {
			_ = c.JSON(he.Code, errBody{Error: he.Message.(string)})
			return
		}
		c.Echo().Logger.Error(err)
		_ = c.JSON(http.StatusInternalServerError, errBody{Error: "internal error"})
	}
}

func badRequest(msg string) error { return echo.NewHTTPError(http.StatusBadRequest, msg) }
