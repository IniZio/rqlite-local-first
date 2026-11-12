package main

import (
	"errors"
	"fmt"
	"testing"

	rqlite "github.com/rqlite/rqlite-go-http"
)

func TestClassify(t *testing.T) {
	sqliteErr := fmt.Errorf("statement 0: %w", &rqlite.StatementError{
		Message: "UNIQUE constraint failed: items.id",
		SQLite:  &rqlite.SQLiteError{Code: 19},
	})
	forwardErr := fmt.Errorf("statement 0: %w", &rqlite.StatementError{
		Message: "dial tcp: lookup rqlite-1: server misbehaving",
	})

	cases := []struct {
		name        string
		in          error
		unavailable bool
	}{
		{"sqlite constraint is a rejection", sqliteErr, false},
		{"failed leader forward queues", forwardErr, true},
		{"request-level error queues", errors.New("leader not found"), true},
	}
	for _, c := range cases {
		got := errors.Is(classify(c.in), ErrUnavailable)
		if got != c.unavailable {
			t.Errorf("%s: unavailable=%v, want %v", c.name, got, c.unavailable)
		}
	}
	if classify(nil) != nil {
		t.Error("nil error must stay nil")
	}
}

func TestOverlayShowsQueuedOpsOnStaleRead(t *testing.T) {
	stale := []Item{{ID: "1", Text: "milk"}, {ID: "2", Text: "eggs"}}
	ops := []Op{
		{Kind: OpAdd, ItemID: "3", Text: "coffee"},
		{Kind: OpSetDone, ItemID: "1", Done: true},
		{Kind: OpDelete, ItemID: "2"},
		{Kind: OpAdd, ItemID: "1", Text: "milk"},
	}
	got := overlay(stale, ops, "C")

	if len(got) != 2 {
		t.Fatalf("want 2 items, got %+v", got)
	}
	if !got[0].Done || !got[0].Pending {
		t.Errorf("queued toggle not shown: %+v", got[0])
	}
	if got[1].ID != "3" || !got[1].Pending {
		t.Errorf("queued add not shown: %+v", got[1])
	}
}
