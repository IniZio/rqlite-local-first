package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	rqlite "github.com/rqlite/rqlite-go-http"
	_ "modernc.org/sqlite"
)

const schema = `CREATE TABLE IF NOT EXISTS items (
	id TEXT PRIMARY KEY,
	text TEXT NOT NULL,
	done INTEGER NOT NULL DEFAULT 0,
	updated_by TEXT NOT NULL,
	updated_at INTEGER NOT NULL
)`

type OpKind string

const (
	OpAdd     OpKind = "add"
	OpSetDone OpKind = "set_done"
	OpDelete  OpKind = "delete"
)

// Op is one user intent. Item ids are minted on the device, so every Op
// replays idempotently: a write that timed out but committed anyway is
// harmless when the outbox sends it again.
type Op struct {
	Kind   OpKind `json:"kind"`
	ItemID string `json:"item_id"`
	Text   string `json:"text,omitempty"`
	Done   bool   `json:"done,omitempty"`
	At     int64  `json:"at"`
}

func (o Op) statement(device string) *rqlite.SQLStatement {
	var s *rqlite.SQLStatement
	switch o.Kind {
	case OpAdd:
		s, _ = rqlite.NewSQLStatement(
			"INSERT OR IGNORE INTO items(id, text, done, updated_by, updated_at) VALUES(?, ?, 0, ?, ?)",
			o.ItemID, o.Text, device, o.At)
	case OpSetDone:
		s, _ = rqlite.NewSQLStatement(
			"UPDATE items SET done = ?, updated_by = ?, updated_at = ? WHERE id = ?",
			o.Done, device, o.At, o.ItemID)
	case OpDelete:
		s, _ = rqlite.NewSQLStatement("DELETE FROM items WHERE id = ?", o.ItemID)
	}
	return s
}

type Item struct {
	ID        string `json:"id"`
	Text      string `json:"text"`
	Done      bool   `json:"done"`
	UpdatedBy string `json:"updated_by"`
	UpdatedAt int64  `json:"updated_at"`
	Pending   bool   `json:"pending"`
}

// ErrUnavailable means the cluster could not commit: no leader, no quorum,
// or the node is unreachable. Callers queue the op instead of failing.
var ErrUnavailable = errors.New("cluster unavailable")

type Store struct {
	device string
	rq     *rqlite.Client
	local  *sql.DB

	// mu orders writes: while the outbox holds ops, new ops queue behind
	// them so this device's intents commit in the order the user made them.
	mu sync.Mutex
}

func OpenStore(device, rqliteURL, localPath string) (*Store, error) {
	rq, err := rqlite.NewClient(rqliteURL, nil)
	if err != nil {
		return nil, err
	}
	local, err := sql.Open("sqlite", localPath+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}
	local.SetMaxOpenConns(1)
	_, err = local.Exec(`CREATE TABLE IF NOT EXISTS outbox (
		seq INTEGER PRIMARY KEY AUTOINCREMENT,
		op TEXT NOT NULL
	)`)
	if err != nil {
		return nil, err
	}
	return &Store{device: device, rq: rq, local: local}, nil
}

func (s *Store) EnsureSchema(ctx context.Context) error {
	return s.execute(ctx, rqlite.SQLStatements{mustStmt(schema)})
}

func mustStmt(sqlText string) *rqlite.SQLStatement {
	st, err := rqlite.NewSQLStatement(sqlText)
	if err != nil {
		panic(err)
	}
	return st
}

func (s *Store) execute(ctx context.Context, stmts rqlite.SQLStatements) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	resp, err := s.rq.Execute(ctx, stmts, &rqlite.ExecuteOptions{Transaction: true})
	if err != nil {
		return fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	return classify(resp.Err())
}

// classify keeps only errors SQLite itself raised as rejections. rqlite
// reports a failed forward to the leader (e.g. the node is cut off) inside
// a 200 response, and that means "could not commit", not "invalid write".
func classify(err error) error {
	var stmtErr *rqlite.StatementError
	if err == nil || (errors.As(err, &stmtErr) && stmtErr.SQLite != nil) {
		return err
	}
	return fmt.Errorf("%w: %v", ErrUnavailable, err)
}

// Apply commits op to the cluster, or queues it locally when the cluster
// cannot commit. It returns queued=true for the latter.
func (s *Store) Apply(ctx context.Context, op Op) (queued bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pending, err := s.pendingCount()
	if err != nil {
		return false, err
	}
	if pending == 0 {
		err = s.execute(ctx, rqlite.SQLStatements{op.statement(s.device)})
		if !errors.Is(err, ErrUnavailable) {
			return false, err
		}
	}
	return true, s.enqueue(op)
}

func (s *Store) enqueue(op Op) error {
	b, err := json.Marshal(op)
	if err != nil {
		return err
	}
	_, err = s.local.Exec("INSERT INTO outbox(op) VALUES(?)", string(b))
	return err
}

func (s *Store) pendingCount() (int, error) {
	var n int
	err := s.local.QueryRow("SELECT COUNT(*) FROM outbox").Scan(&n)
	return n, err
}

func (s *Store) pending() ([]int64, []Op, error) {
	rows, err := s.local.Query("SELECT seq, op FROM outbox ORDER BY seq")
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var seqs []int64
	var ops []Op
	for rows.Next() {
		var seq int64
		var raw string
		if err := rows.Scan(&seq, &raw); err != nil {
			return nil, nil, err
		}
		var op Op
		if err := json.Unmarshal([]byte(raw), &op); err != nil {
			return nil, nil, err
		}
		seqs = append(seqs, seq)
		ops = append(ops, op)
	}
	return seqs, ops, rows.Err()
}

// Drain sends the whole outbox as one rqlite transaction, then clears it.
// A crash between the two steps replays the batch, which is safe because
// every Op is idempotent.
func (s *Store) Drain(ctx context.Context) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	seqs, ops, err := s.pending()
	if err != nil || len(ops) == 0 {
		return 0, err
	}
	stmts := make(rqlite.SQLStatements, len(ops))
	for i, op := range ops {
		stmts[i] = op.statement(s.device)
	}
	if err := s.execute(ctx, stmts); err != nil {
		return 0, err
	}
	_, err = s.local.Exec("DELETE FROM outbox WHERE seq <= ?", seqs[len(seqs)-1])
	return len(ops), err
}

type Snapshot struct {
	Items     []Item `json:"items"`
	ReadLevel string `json:"read_level"`
	Online    bool   `json:"online"`
	Pending   int    `json:"pending"`
	Error     string `json:"error,omitempty"`
}

// Read tries a linearizable read, which needs a leader backed by a quorum.
// Without one it reads the local node's copy (level=none): possibly stale,
// but available. Queued ops are overlaid so the user sees their own writes.
func (s *Store) Read(ctx context.Context) Snapshot {
	snap := Snapshot{Online: true, ReadLevel: "linearizable"}
	items, err := s.query(ctx, rqlite.ReadConsistencyLevelLinearizable)
	if err != nil {
		snap.Online, snap.ReadLevel, snap.Error = false, "none", err.Error()
		items, err = s.query(ctx, rqlite.ReadConsistencyLevelNone)
		if err != nil {
			snap.ReadLevel, snap.Error = "unreachable", err.Error()
		}
	}
	_, ops, perr := s.pending()
	if perr != nil {
		snap.Error = perr.Error()
	}
	snap.Pending = len(ops)
	snap.Items = overlay(items, ops, s.device)
	return snap
}

func (s *Store) query(ctx context.Context, level rqlite.ReadConsistencyLevel) ([]Item, error) {
	ctx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	resp, err := s.rq.Query(ctx,
		rqlite.SQLStatements{mustStmt("SELECT id, text, done, updated_by, updated_at FROM items ORDER BY updated_at, id")},
		&rqlite.QueryOptions{Level: level, LinearizableTimeout: time.Second})
	if err != nil {
		return nil, err
	}
	if err := resp.Err(); err != nil {
		return nil, err
	}
	var items []Item
	for _, res := range resp.GetQueryResults() {
		for _, row := range res.Values {
			items = append(items, Item{
				ID:        fmt.Sprint(row[0]),
				Text:      fmt.Sprint(row[1]),
				Done:      fmt.Sprint(row[2]) == "1",
				UpdatedBy: fmt.Sprint(row[3]),
				UpdatedAt: toInt(row[4]),
			})
		}
	}
	return items, nil
}

func toInt(v any) int64 {
	if n, ok := v.(json.Number); ok {
		i, _ := n.Int64()
		return i
	}
	return 0
}

func overlay(items []Item, ops []Op, device string) []Item {
	for _, op := range ops {
		switch op.Kind {
		case OpAdd:
			if slices.ContainsFunc(items, func(it Item) bool { return it.ID == op.ItemID }) {
				continue
			}
			items = append(items, Item{ID: op.ItemID, Text: op.Text, UpdatedBy: device, UpdatedAt: op.At, Pending: true})
		case OpSetDone:
			for i := range items {
				if items[i].ID == op.ItemID {
					items[i].Done, items[i].Pending = op.Done, true
				}
			}
		case OpDelete:
			kept := items[:0]
			for _, it := range items {
				if it.ID != op.ItemID {
					kept = append(kept, it)
				}
			}
			items = kept
		}
	}
	if items == nil {
		items = []Item{}
	}
	return items
}

type NodeView struct {
	ID        string `json:"id"`
	Reachable bool   `json:"reachable"`
	Leader    bool   `json:"leader"`
}

type ClusterView struct {
	RaftState string     `json:"raft_state"`
	Nodes     []NodeView `json:"nodes"`
}

func (s *Store) Cluster(ctx context.Context) ClusterView {
	ctx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	view := ClusterView{RaftState: "unreachable", Nodes: []NodeView{}}
	if raw, err := s.rq.Status(ctx); err == nil {
		var st struct {
			Store struct {
				Raft struct {
					State string `json:"state"`
				} `json:"raft"`
			} `json:"store"`
		}
		if json.Unmarshal(raw, &st) == nil {
			view.RaftState = st.Store.Raft.State
		}
	}
	if raw, err := s.rq.Nodes(ctx, &rqlite.NodeOptions{Version: "2", Timeout: time.Second}); err == nil {
		var nodes struct {
			Nodes []NodeView `json:"nodes"`
		}
		if json.Unmarshal(raw, &nodes) == nil {
			view.Nodes = nodes.Nodes
		}
	}
	return view
}
