package store

import "github.com/jackc/pgx/v5"

// pgxTx is the transaction interface used across store helpers.
type pgxTx = pgx.Tx
