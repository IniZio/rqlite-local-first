package main

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
)

//go:embed index.html
var static embed.FS

func main() {
	device := env("DEVICE", "A")
	rqliteURL := env("RQLITE_URL", "http://localhost:4001")
	store, err := OpenStore(device, rqliteURL, env("DATA_DIR", ".")+"/outbox.db")
	if err != nil {
		slog.Error("open store", "err", err)
		os.Exit(1)
	}

	ctx := context.Background()
	go keepSchema(ctx, store)
	go drainLoop(ctx, store)

	mux := http.NewServeMux()
	mux.Handle("GET /", http.FileServerFS(static))
	mux.HandleFunc("GET /api/state", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"device":  device,
			"node":    rqliteURL,
			"data":    store.Read(r.Context()),
			"cluster": store.Cluster(r.Context()),
		})
	})
	mux.HandleFunc("POST /api/items", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Text string `json:"text"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || strings.TrimSpace(body.Text) == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "text required"})
			return
		}
		apply(w, r, store, Op{Kind: OpAdd, ItemID: uuid.NewString(), Text: strings.TrimSpace(body.Text)})
	})
	mux.HandleFunc("POST /api/items/{id}/done", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Done bool `json:"done"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "done required"})
			return
		}
		apply(w, r, store, Op{Kind: OpSetDone, ItemID: r.PathValue("id"), Done: body.Done})
	})
	mux.HandleFunc("DELETE /api/items/{id}", func(w http.ResponseWriter, r *http.Request) {
		apply(w, r, store, Op{Kind: OpDelete, ItemID: r.PathValue("id")})
	})

	slog.Info("listening", "device", device, "rqlite", rqliteURL, "addr", ":8080")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		slog.Error("serve", "err", err)
		os.Exit(1)
	}
}

func apply(w http.ResponseWriter, r *http.Request, store *Store, op Op) {
	op.At = time.Now().UnixMilli()
	queued, err := store.Apply(r.Context(), op)
	if err != nil {
		slog.Error("write rejected", "op", op.Kind, "item", op.ItemID, "err", err)
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": err.Error()})
		return
	}
	status := "committed"
	if queued {
		status = "queued"
	}
	slog.Info("write", "op", op.Kind, "item", op.ItemID, "status", status)
	writeJSON(w, http.StatusOK, map[string]string{"id": op.ItemID, "status": status})
}

func keepSchema(ctx context.Context, store *Store) {
	for {
		err := store.EnsureSchema(ctx)
		if err == nil {
			slog.Info("schema ready")
			return
		}
		if !errors.Is(err, ErrUnavailable) {
			slog.Error("schema", "err", err)
		}
		time.Sleep(time.Second)
	}
}

func drainLoop(ctx context.Context, store *Store) {
	for range time.Tick(time.Second) {
		n, err := store.Drain(ctx)
		if n > 0 {
			slog.Info("outbox drained", "ops", n)
		}
		if err != nil && !errors.Is(err, ErrUnavailable) {
			slog.Error("drain", "err", err)
		}
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
