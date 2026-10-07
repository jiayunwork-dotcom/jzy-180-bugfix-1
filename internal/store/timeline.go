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
	eventID int64 // >0 for an event (resume or flags; shared stream_events id)
	isEvent bool
	isFlags bool
}

// loadedBatch carries the inputs needed to fold a batch.
type loadedBatch struct {
	row BatchRow
	in  statemachine.BatchInput
}

// loadedTimeline is every row that participates in a stream's fold.
type loadedTimeline struct {
	batches []loadedBatch
	resumes []replay.ResumeEvent
	flags   []replay.FlagsEvent
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
		ts := at.UTC().Format(time.RFC3339Nano)
		switch kind {
		case "resume":
			out.resumes = append(out.resumes, replay.ResumeEvent{ID: id, OccurredAt: ts})
		case "flags":
			if stable == nil || approval == nil {
				eventRows.Close()
				return loadedTimeline{}, errCorruptFlagsEvent(id)
			}
			out.flags = append(out.flags, replay.FlagsEvent{
				ID:         id,
				OccurredAt: ts,
				Flags: replay.Flags{
					ProductionStable:   *stable,
					SupervisorApproval: *approval,
				},
			})
		}
	}
	eventRows.Close()
	if err := eventRows.Err(); err != nil {
		return loadedTimeline{}, err
	}
	return out, nil
}

func errCorruptFlagsEvent(id int64) error {
	return &flagsEventError{id: id}
}

type flagsEventError struct{ id int64 }

func (e *flagsEventError) Error() string {
	return "corrupt flags event (missing flag values)"
}

// buildOrderedItems produces the unified, deterministic fold order.
func buildOrderedItems(lt loadedTimeline) []timelineItem {
	items := make([]timelineItem, 0, len(lt.batches)+len(lt.resumes)+len(lt.flags))
	for _, e := range lt.resumes {
		t, _ := time.Parse(time.RFC3339Nano, e.OccurredAt)
		items = append(items, timelineItem{at: t, eventID: e.ID, isEvent: true})
	}
	for _, e := range lt.flags {
		t, _ := time.Parse(time.RFC3339Nano, e.OccurredAt)
		items = append(items, timelineItem{at: t, eventID: e.ID, isEvent: true, isFlags: true})
	}
	for _, b := range lt.batches {
		items = append(items, timelineItem{
			at:      b.row.InspectedAt,
			batchID: b.row.ID,
		})
	}
	// Must match replay's ordering: time; batches before events at the same
	// instant; events (resume/flags) by their shared stream_events id; batches
	// by id.
	sort.SliceStable(items, func(i, j int) bool {
		if !items[i].at.Equal(items[j].at) {
			return items[i].at.Before(items[j].at)
		}
		if items[i].isEvent != items[j].isEvent {
			return !items[i].isEvent // batch before event at the same instant
		}
		if items[i].isEvent {
			return items[i].eventID < items[j].eventID
		}
		return items[i].batchID < items[j].batchID
	})
	return items
}
