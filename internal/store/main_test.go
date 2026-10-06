package store_test

import (
	"context"
	"os"
	"testing"
	"time"

	"qcinspect/internal/sampling"
	"qcinspect/internal/store"
)

func testDSN() string {
	if v := os.Getenv("DATABASE_URL_STORE"); v != "" {
		return v
	}
	return "postgres://qc@127.0.0.1:5432/qcinspect_store?sslmode=disable"
}

func adminDSN() string {
	if v := os.Getenv("DATABASE_URL_ADMIN"); v != "" {
		return v
	}
	return "postgres://qc@127.0.0.1:5432/postgres?sslmode=disable"
}

func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	ctx := context.Background()
	// Ensure the package-private database exists so parallel test packages
	// never truncate each other's data.
	ensureDatabase(t)
	st, err := store.New(ctx, testDSN())
	if err != nil {
		t.Skipf("postgresql not available: %v", err)
	}
	t.Cleanup(st.Close)
	cleanDB(ctx, t, st)
	return st
}

func ensureDatabase(t *testing.T) {
	t.Helper()
	adm, err := store.New(context.Background(), adminDSN())
	if err != nil {
		t.Skipf("postgresql not available: %v", err)
	}
	defer adm.Close()
	if _, err := adm.Pool().Exec(context.Background(),
		`CREATE DATABASE qcinspect_store`); err != nil {
		// "already exists" is expected; anything else is surfaced on connect.
		t.Logf("create database: %v", err)
	}
}

func cleanDB(ctx context.Context, t *testing.T, st *store.Store) {
	t.Helper()
	pool := st.Pool()
	for _, tbl := range []string{"stream_checkpoints", "batches", "stream_events", "streams", "plans"} {
		if _, err := pool.Exec(ctx, "TRUNCATE TABLE "+tbl+" RESTART IDENTITY CASCADE"); err != nil {
			t.Fatal(err)
		}
	}
}

func mustPlan(t *testing.T, st *store.Store, name string, pl sampling.Plan) int64 {
	t.Helper()
	pr, err := st.CreatePlan(context.Background(), name, pl)
	if err != nil {
		t.Fatalf("create plan %s: %v", name, err)
	}
	return pr.ID
}

func mkTriplePlans(t *testing.T, st *store.Store, suffix string) (int64, int64, int64) {
	n := mustPlan(t, st, "normal_"+suffix, sampling.Plan{Kind: sampling.Single, N: 8, C: 3})
	ti := mustPlan(t, st, "tight_"+suffix, sampling.Plan{Kind: sampling.Single, N: 8, C: 1})
	r := mustPlan(t, st, "reduced_"+suffix, sampling.Plan{Kind: sampling.Single, N: 8, C: 5})
	return n, ti, r
}

func mkDoublePlans(t *testing.T, st *store.Store, suffix string) (int64, int64, int64) {
	n := mustPlan(t, st, "dn_"+suffix, sampling.Plan{
		Kind: sampling.Double, N1: 8, C1: 1, R1: 4, N2: 8, C2: 4})
	ti := mustPlan(t, st, "dt_"+suffix, sampling.Plan{
		Kind: sampling.Double, N1: 8, C1: 0, R1: 3, N2: 8, C2: 3})
	r := mustPlan(t, st, "dr_"+suffix, sampling.Plan{
		Kind: sampling.Double, N1: 8, C1: 3, R1: 6, N2: 8, C2: 6})
	return n, ti, r
}

func at(hour int) time.Time {
	return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).
		Add(time.Duration(hour) * time.Hour).Truncate(time.Microsecond)
}

func intp(x int) *int { return &x }
