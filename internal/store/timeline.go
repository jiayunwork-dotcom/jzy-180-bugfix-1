package store

import (
	"context"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"

	"qcinspect/internal/replay"
	"qcinspect/internal/statemachine"
)

// timelineItem is one chronological element loaded for a stream.
type timelineItem struct {
	at        time.Time
	batchID   int64 // >0 for a batch
	eventID   int64 // >0 for an event
	resume    bool
	flagEvent bool
}

// loadedBatch carries the inputs needed to fold a batch.
type loadedBatch struct {
	row BatchRow
	in  statemachine.BatchInput
}

// loadedTimeline is every fold input for a stream: resume events, flag events
// and batches, in no particular order (the caller builds the ordered view).
type loadedTimeline struct {
	resumes    []replay.ResumeEvent
	flagEvents []replay.FlagEvent
	batches    []loadedBatch
}

// loadTimeline reads all batches and events for a stream in a transaction.
func loadTimeline(ctx context.Context, tx pgx.Tx, streamID int64) (loadedTimeline, error) {
	var out loadedTimeline

	batchRows, err := tx.Query(ctx,
		`SELECT id, stream_id, batch_no, inspected_at, d1, d2, result
		 FROM batches WHERE stream_id=$1`, streamID)
	if err != nil {
		return loadedTimeline{}, err
	}
	for batchRows.Next() {
		var br BatchRow
		if err := batchRows.Scan(&br.ID, &br.StreamID, &br.BatchNo,
			&br.InspectedAt, &br.D1, &br.D2, &br.Result); err != nil {
			batchRows.Close()
			return loadedTimeline{}, err
		}
		d2 := 0
		if br.D2 != nil {
			d2 = *br.D2
		}
		out.batches = append(out.batches, loadedBatch{
			row: br,
			in: statemachine.BatchInput{
				ID:          br.ID,
				BatchNo:     br.BatchNo,
				InspectedAt: br.InspectedAt.UTC().Format(time.RFC3339Nano),
				D1:          br.D1,
				D2:          d2,
			},
		})
	}
	batchRows.Close()
	if err := batchRows.Err(); err != nil {
		return loadedTimeline{}, err
	}

	eventRows, err := tx.Query(ctx,
		`SELECT id, kind, occurred_at, production_stable, supervisor_approval
		 FROM stream_events WHERE stream_id=$1 ORDER BY occurred_at, id`, streamID)
	if err != nil {
		return loadedTimeline{}, err
	}
	for eventRows.Next() {
		var id int64
		var kind string
		var at time.Time
		var stable, approval *bool
		if err := eventRows.Scan(&id, &kind, &at, &stable, &approval); err != nil {
			eventRows.Close()
			return loadedTimeline{}, err
		}
		switch kind {
		case "resume":
			out.resumes = append(out.resumes, replay.ResumeEvent{
				ID:         id,
				OccurredAt: at.UTC().Format(time.RFC3339Nano),
			})
		case "flags":
			if stable == nil || approval == nil {
				eventRows.Close()
				return loadedTimeline{}, &corruptFlagEventError{id: id}
			}
			out.flagEvents = append(out.flagEvents, replay.FlagEvent{
				ID:                 id,
				OccurredAt:         at.UTC().Format(time.RFC3339Nano),
				ProductionStable:   *stable,
				SupervisorApproval: *approval,
			})
		}
	}
	eventRows.Close()
	if err := eventRows.Err(); err != nil {
		return loadedTimeline{}, err
	}
	return out, nil
}

type corruptFlagEventError struct{ id int64 }

func (e *corruptFlagEventError) Error() string {
	return "corrupt flags event: missing flag columns"
}

// buildOrderedItems produces the unified, deterministic fold order. It must
// match replay.Timeline.items' ordering exactly (time, then batch < resume <
// flags, then source id).
func buildOrderedItems(tl loadedTimeline) []timelineItem {
	items := make([]timelineItem, 0, len(tl.resumes)+len(tl.flagEvents)+len(tl.batches))
	for _, e := range tl.resumes {
		t, _ := time.Parse(time.RFC3339Nano, e.OccurredAt)
		items = append(items, timelineItem{at: t, eventID: e.ID, resume: true})
	}
	for _, f := range tl.flagEvents {
		t, _ := time.Parse(time.RFC3339Nano, f.OccurredAt)
		items = append(items, timelineItem{at: t, eventID: f.ID, flagEvent: true})
	}
	for _, b := range tl.batches {
		items = append(items, timelineItem{
			at:      b.row.InspectedAt,
			batchID: b.row.ID,
		})
	}
	sort.SliceStable(items, func(i, j int) bool {
		if !items[i].at.Equal(items[j].at) {
			return items[i].at.Before(items[j].at)
		}
		ci, cj := items[i].eventClass(), items[j].eventClass()
		if ci != cj {
			return ci < cj
		}
		if items[i].batchID != items[j].batchID {
			return items[i].batchID < items[j].batchID
		}
		return items[i].eventID < items[j].eventID
	})
	return items
}

// eventClass orders batches (0) before resume events (1) before flag events
// (2) at equal timestamps.
func (it timelineItem) eventClass() int {
	switch {
	case it.flagEvent:
		return 2
	case it.resume:
		return 1
	default:
		return 0
	}
}
