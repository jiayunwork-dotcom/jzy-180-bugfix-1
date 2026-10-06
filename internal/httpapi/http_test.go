package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/labstack/echo/v4"

	"qcinspect/internal/httpapi"
	"qcinspect/internal/sampling"
	"qcinspect/internal/store"
)

func dsn() string {
	if v := os.Getenv("DATABASE_URL_HTTP"); v != "" {
		return v
	}
	return "postgres://qc@127.0.0.1:5432/qcinspect_http?sslmode=disable"
}

func setup(t *testing.T) (*echo.Echo, *store.Store) {
	t.Helper()
	// Package-private database so `go test ./...` never truncates another
	// package's data while it runs in parallel.
	adm, err := store.New(context.Background(),
		"postgres://qc@127.0.0.1:5432/postgres?sslmode=disable")
	if err != nil {
		t.Skipf("postgresql not available: %v", err)
	}
	if _, err := adm.Pool().Exec(context.Background(),
		`CREATE DATABASE qcinspect_http`); err != nil {
		t.Logf("create database: %v", err)
	}
	adm.Close()

	st, err := store.New(context.Background(), dsn())
	if err != nil {
		t.Skipf("postgresql not available: %v", err)
	}
	t.Cleanup(st.Close)
	for _, tbl := range []string{"stream_checkpoints", "batches", "stream_events", "streams", "plans"} {
		if _, err := st.Pool().Exec(context.Background(),
			"TRUNCATE TABLE "+tbl+" RESTART IDENTITY CASCADE"); err != nil {
			t.Fatal(err)
		}
	}
	return httpapi.New(st), st
}

func doJSON(t *testing.T, e *echo.Echo, method, path string, body any) (int, map[string]any) {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	var out map[string]any
	if rec.Body.Len() > 0 {
		_ = json.Unmarshal(rec.Body.Bytes(), &out)
	}
	return rec.Code, out
}

func TestPlanCRUDAndCalculations(t *testing.T) {
	e, _ := setup(t)

	// Invalid plan: c >= n -> 422 with field detail.
	code, body := doJSON(t, e, "POST", "/api/plans", map[string]any{
		"name": "bad", "definition": sampling.Plan{Kind: sampling.Single, N: 5, C: 9}})
	if code != 422 {
		t.Fatalf("status=%d body=%v", code, body)
	}
	fields, _ := body["fields"].([]any)
	if len(fields) == 0 {
		t.Fatal("expected field errors")
	}

	// Create n=80, c=2.
	code, body = doJSON(t, e, "POST", "/api/plans", map[string]any{
		"name": "p80", "definition": sampling.Plan{Kind: sampling.Single, N: 80, C: 2}})
	if code != 201 {
		t.Fatalf("create status=%d body=%v", code, body)
	}
	id := int64(body["id"].(float64))

	// By-name lookup endpoint (list + filter check through get).
	code, body = doJSON(t, e, "GET", "/api/plans/"+itoa(id), nil)
	if code != 200 {
		t.Fatalf("get: %d %v", code, body)
	}

	// Pa at p=0 and p=1 via plan path.
	code, body = doJSON(t, e, "POST", "/api/plans/"+itoa(id)+"/pa", map[string]any{"p": 0})
	if code != 200 || body["pa"].(float64) != 1 {
		t.Fatalf("Pa(0): %d %v", code, body)
	}
	code, body = doJSON(t, e, "POST", "/api/plans/"+itoa(id)+"/pa", map[string]any{"p": 1})
	if code != 200 || body["pa"].(float64) != 0 {
		t.Fatalf("Pa(1): %d %v", code, body)
	}
	code, body = doJSON(t, e, "POST", "/api/plans/"+itoa(id)+"/pa", map[string]any{"p": 0.02})
	if code != 200 {
		t.Fatal(body)
	}
	if got := body["pa"].(float64); abs(got-0.7844188871) > 1e-6 {
		t.Errorf("Pa(0.02)=%.8f", got)
	}

	// OC curve with points cap.
	code, body = doJSON(t, e, "POST", "/api/plans/"+itoa(id)+"/oc",
		map[string]any{"p_min": 0.0, "p_max": 0.2, "points": 501})
	if code != 422 {
		t.Fatalf("points cap: %d", code)
	}
	code, body = doJSON(t, e, "POST", "/api/plans/"+itoa(id)+"/oc",
		map[string]any{"p_min": 0.0, "p_max": 0.2, "points": 201})
	if code != 200 {
		t.Fatal(body)
	}
	if pts, _ := body["points"].([]any); len(pts) != 201 {
		t.Fatalf("points=%d", len(pts))
	}

	// Risks.
	code, body = doJSON(t, e, "POST", "/api/plans/"+itoa(id)+"/risks",
		map[string]any{"aql": 0.01, "ltpd": 0.08})
	if code != 200 {
		t.Fatal(body)
	}
	r, _ := body["risks"].(map[string]any)
	if r["alpha"].(float64) <= 0 || r["beta"].(float64) <= 0 {
		t.Fatal("risks must be positive here")
	}
	// aql >= ltpd rejected.
	code, _ = doJSON(t, e, "POST", "/api/plans/"+itoa(id)+"/risks",
		map[string]any{"aql": 0.1, "ltpd": 0.1})
	if code != 422 {
		t.Fatalf("aql>=ltpd status=%d", code)
	}

	// Inline plan AOQ requires lot size.
	code, body = doJSON(t, e, "POST", "/api/calculate/aoq", map[string]any{
		"plan": sampling.Plan{Kind: sampling.Single, N: 80, C: 2}, "p": 0.02})
	if code != 422 {
		t.Fatalf("aoq without N status=%d", code)
	}

	// Delete bound plan rules are exercised in stream tests.
}

func TestDesignEndpoints(t *testing.T) {
	e, _ := setup(t)
	code, body := doJSON(t, e, "POST", "/api/design/single", map[string]any{
		"aql": 0.01, "alpha": 0.05, "ltpd": 0.06, "beta": 0.10})
	if code != 200 {
		t.Fatalf("design single: %d %v", code, body)
	}
	pl, _ := body["plan"].(map[string]any)
	n := int(pl["n"].(float64))
	c := int(pl["c"].(float64))
	if n <= 0 || c < 0 || c >= n {
		t.Fatalf("bad designed plan %+v", pl)
	}
	// No-feasible reports a clear error.
	code, body = doJSON(t, e, "POST", "/api/design/single", map[string]any{
		"aql": 0.01, "alpha": 0.05, "ltpd": 0.011, "beta": 0.001, "max_n": 5})
	if code != 422 || body["code"] != "no_feasible_plan" {
		t.Fatalf("infeasible: %d %v", code, body)
	}
	code, body = doJSON(t, e, "POST", "/api/design/double", map[string]any{
		"aql": 0.01, "alpha": 0.05, "ltpd": 0.08, "beta": 0.10})
	if code != 200 {
		t.Fatalf("design double: %d %v", code, body)
	}
	dp, _ := body["plan"].(map[string]any)
	n1, n2 := int(dp["n1"].(float64)), int(dp["n2"].(float64))
	if n2 != n1 && n2 != 2*n1 {
		t.Fatalf("n2=%d not n1 or 2*n1 (%d)", n2, n1)
	}
}

func TestFullStreamFlowOverHTTP(t *testing.T) {
	e, _ := setup(t)
	mk := func(name string, n, c int) int64 {
		code, body := doJSON(t, e, "POST", "/api/plans", map[string]any{
			"name": name, "definition": sampling.Plan{Kind: sampling.Single, N: n, C: c}})
		if code != 201 {
			t.Fatal(body)
		}
		return int64(body["id"].(float64))
	}
	nID, tID, rID := mk("hn", 8, 3), mk("ht", 8, 1), mk("hr", 8, 5)

	code, body := doJSON(t, e, "POST", "/api/streams", map[string]any{
		"name":           "flow",
		"normal_plan_id": nID, "tightened_plan_id": tID, "reduced_plan_id": rID,
	})
	if code != 201 {
		t.Fatal(body)
	}
	sid := itoa(int64(body["id"].(float64)))

	add := func(no, ts string, d int) (int, map[string]any) {
		return doJSON(t, e, "POST", "/api/streams/"+sid+"/batches", map[string]any{
			"batch_no": no, "inspected_at": ts, "d1": d})
	}
	if c, b := add("L1", "2026-01-01T08:00:00Z", 4); c != 201 {
		t.Fatalf("L1: %d %v", c, b)
	}
	if c, b := add("L2", "2026-01-02T08:00:00Z", 4); c != 201 {
		t.Fatalf("L2: %d %v", c, b)
	}
	// Third reject within the window is already on tightened; read stream.
	code, body = doJSON(t, e, "GET", "/api/streams/"+sid, nil)
	if code != 200 {
		t.Fatal(body)
	}
	stObj, _ := body["state"].(map[string]any)
	if stObj["severity"] != "tightened" {
		t.Fatalf("severity=%v", stObj["severity"])
	}
	batches, _ := body["batches"].([]any)
	if len(batches) != 2 {
		t.Fatalf("batches=%d", len(batches))
	}
	first, _ := batches[0].(map[string]any)
	firstRes, _ := first["result"].(map[string]any)
	if firstRes["severity"] != "normal" {
		t.Errorf("first batch severity=%v", firstRes["severity"])
	}

	// Invalid count (d1 > sample size) -> 422 with field.
	if c, b := add("bad", "2026-01-03T08:00:00Z", 99); c != 422 {
		t.Fatalf("bad d1: %d %v", c, b)
	}

	// Backdated insert: L0 before L1; state must stay consistent (endpoint
	// returns the whole recomputed timeline).
	if c, b := doJSON(t, e, "POST", "/api/streams/"+sid+"/batches", map[string]any{
		"batch_no": "L0", "inspected_at": "2025-12-31T08:00:00Z", "d1": 0}); c != 201 {
		t.Fatalf("backfill: %d %v", c, b)
	} else {
		bs, _ := b["batches"].([]any)
		if bs[0].(map[string]any)["batch_no"] != "L0" {
			t.Fatal("backfilled batch must be first in timeline")
		}
	}
}

func itoa(i int64) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

var _ = http.StatusOK
