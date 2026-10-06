// Command server runs the QC acceptance-sampling service.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"qcinspect/internal/httpapi"
	"qcinspect/internal/store"
)

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://qc:qc@localhost:5432/qcinspect?sslmode=disable"
	}
	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	// Wait for PostgreSQL to accept connections (container ordering).
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var st *store.Store
	var err error
	for attempt := 1; ; attempt++ {
		st, err = store.New(ctx, dsn)
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			log.Fatalf("database unreachable: %v", err)
		}
		log.Printf("waiting for database (attempt %d): %v", attempt, err)
		time.Sleep(time.Second)
	}
	defer st.Close()

	e := httpapi.New(st)
	srv := &http.Server{Addr: addr, Handler: e}

	go func() {
		log.Printf("listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	shutdownCtx, c := context.WithTimeout(context.Background(), 10*time.Second)
	defer c()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown error: %v", err)
	}
}
