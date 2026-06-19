package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// runWatchMode streams metrics continuously as newline-delimited JSON (one full
// MetricsSnapshot per line) using a single warm Collector, so rate metrics
// (network, disk IO) stay accurate across ticks.
//
//   - listen == "" : write the NDJSON stream to stdout (used by the MoleUI
//     user-level `status-go --watch --interval <d>` spawn).
//   - listen != "" : serve the same NDJSON stream over a unix socket at that
//     path (used by the root LaunchDaemon `status-go --watch --listen <sock>`),
//     so the unprivileged app can read fully-populated GPU/temp/fan metrics.
func runWatchMode(interval time.Duration, listen string) {
	if listen != "" {
		runWatchSocket(interval, listen)
		return
	}
	runWatchStdout(interval)
}

// watchState mirrors the TUI's collection cadence (cmd/status/main.go): a full
// collect priming the enrichment cache, then mostly fast collects that inherit
// the cached slow-changing fields, with periodic process/full refreshes.
type watchState struct {
	ready         bool
	lastFullAt    time.Time
	lastProcessAt time.Time
}

func (s *watchState) nextMode(now time.Time) collectionMode {
	// Mirror the TUI cadence: a fast first paint, then a full collect on the
	// next tick to fill the enrichment cache (GPU/thermal/batteries/disks),
	// then mostly fast collects with periodic process/full refreshes.
	if !s.ready {
		return collectionFast
	}
	if s.lastFullAt.IsZero() || now.Sub(s.lastFullAt) >= slowRefreshInterval {
		return collectionFull
	}
	if s.lastProcessAt.IsZero() || now.Sub(s.lastProcessAt) >= processWatchInterval {
		return collectionProcess
	}
	return collectionFast
}

func (s *watchState) collect(c *Collector) MetricsSnapshot {
	now := time.Now()
	mode := s.nextMode(now)

	var (
		snap MetricsSnapshot
		err  error
	)
	switch mode {
	case collectionFull:
		snap, err = c.Collect()
	case collectionProcess:
		snap, err = c.CollectProcesses()
	default:
		snap, err = c.CollectFast()
	}

	if err == nil {
		if mode == collectionFull {
			s.lastFullAt = snap.CollectedAt
		}
		if mode == collectionProcess || mode == collectionFull {
			s.lastProcessAt = snap.CollectedAt
		}
		s.ready = true
	}
	return snap
}

// runWatchStdout emits the first snapshot immediately (so the consumer paints
// without waiting a full interval), then one per interval. Exits cleanly when
// stdout closes (parent process gone).
func runWatchStdout(interval time.Duration) {
	collector := NewCollector(processWatchOptionsFromFlags())
	enc := json.NewEncoder(os.Stdout)
	var st watchState

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		snap := st.collect(collector)
		if err := enc.Encode(snap); err != nil {
			return // stdout closed → parent died; nothing left to feed.
		}
		<-ticker.C
	}
}

// runWatchSocket serves the NDJSON stream over a unix socket. A single producer
// goroutine collects once per tick (keeping the one shared Collector warm and
// race-free) and broadcasts each snapshot to every connected client; dead
// clients are dropped on write error.
func runWatchSocket(interval time.Duration, socketPath string) {
	// Clear any stale socket from a previous (crashed) run before binding.
	_ = os.Remove(socketPath)

	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "status: listen on %s failed: %v\n", socketPath, err)
		os.Exit(1)
	}
	// The daemon runs as root; the unprivileged app must be able to connect.
	if err := os.Chmod(socketPath, 0o666); err != nil {
		fmt.Fprintf(os.Stderr, "status: chmod %s failed: %v\n", socketPath, err)
	}

	// Clean up the socket on SIGINT/SIGTERM (launchd bootout sends SIGTERM).
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		_ = ln.Close()
		_ = os.Remove(socketPath)
		os.Exit(0)
	}()

	var (
		mu    sync.Mutex
		conns = map[net.Conn]*json.Encoder{}
	)

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return // listener closed (shutdown).
			}
			mu.Lock()
			conns[conn] = json.NewEncoder(conn)
			mu.Unlock()
		}
	}()

	collector := NewCollector(processWatchOptionsFromFlags())
	var st watchState

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		snap := st.collect(collector) // always collect → Collector stays warm.
		mu.Lock()
		for conn, enc := range conns {
			if err := enc.Encode(snap); err != nil {
				_ = conn.Close()
				delete(conns, conn)
			}
		}
		mu.Unlock()
		<-ticker.C
	}
}
