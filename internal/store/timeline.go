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
	at      time.Time
	batchID int64 // >0 for a batch
	eventID int64 // >0 for an event
	isEvent bool
}

// loadedBatch carries the inputs needed to fold a batch.
type loadedBatch struct {
	row BatchRow
	in  statemachine.BatchInput
}

// loadTimeline reads all batches and events for a stream in a transaction and
// returns them in the exact fold order.
func loadTimeline(ctx context.Context, tx pgx.Tx, streamID int64) (
	[]replay.ResumeEvent, []loadedBatch, error) {

	batchRows, err := tx.Query(ctx,
		`SELECT id, stream_id, batch_no, inspected_at, d1, d2, result
		 FROM batches WHERE stream_id=$1`, streamID)
	if err != nil {
		return nil, nil, err
	}
	var batches []loadedBatch
	for batchRows.Next() {
		var br BatchRow
		if err := batchRows.Scan(&br.ID, &br.StreamID, &br.BatchNo,
			&br.InspectedAt, &br.D1, &br.D2, &br.Result); err != nil {
			batchRows.Close()
			return nil, nil, err
		}
		d2 := 0
		if br.D2 != nil {
			d2 = *br.D2
		}
		batches = append(batches, loadedBatch{
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
		return nil, nil, err
	}

	eventRows, err := tx.Query(ctx,
		`SELECT id, stream_id, kind, occurred_at
		 FROM stream_events WHERE stream_id=$1 ORDER BY occurred_at, id`, streamID)
	if err != nil {
		return nil, nil, err
	}
	var events []replay.ResumeEvent
	for eventRows.Next() {
		var id, sid int64
		var kind string
		var at time.Time
		if err := eventRows.Scan(&id, &sid, &kind, &at); err != nil {
			eventRows.Close()
			return nil, nil, err
		}
		if kind == "resume" {
			events = append(events, replay.ResumeEvent{
				ID:         id,
				OccurredAt: at.UTC().Format(time.RFC3339Nano),
			})
		}
	}
	eventRows.Close()
	if err := eventRows.Err(); err != nil {
		return nil, nil, err
	}
	return events, batches, nil
}

// earliestItemIndex returns the fold-order index of the earliest affected
// item: the smallest timeline index whose at-time is >= minTime. The caller
// also passes any deleted/changed id directly; here we only locate by time.
func earliestItemIndex(items []timelineItem, minTime time.Time) int {
	idx := sort.Search(len(items), func(i int) bool {
		return !items[i].at.Before(minTime)
	})
	return idx
}

// buildOrderedItems produces the unified, deterministic fold order.
func buildOrderedItems(events []replay.ResumeEvent, batches []loadedBatch) []timelineItem {
	items := make([]timelineItem, 0, len(events)+len(batches))
	for _, e := range events {
		t, _ := time.Parse(time.RFC3339Nano, e.OccurredAt)
		items = append(items, timelineItem{at: t, eventID: e.ID, isEvent: true})
	}
	for _, b := range batches {
		items = append(items, timelineItem{
			at:      b.row.InspectedAt,
			batchID: b.row.ID,
		})
	}
	sort.SliceStable(items, func(i, j int) bool {
		if !items[i].at.Equal(items[j].at) {
			return items[i].at.Before(items[j].at)
		}
		if items[i].isEvent != items[j].isEvent {
			return !items[i].isEvent // batch before event at the same instant
		}
		if items[i].batchID != items[j].batchID {
			return items[i].batchID < items[j].batchID
		}
		return items[i].eventID < items[j].eventID
	})
	return items
}
