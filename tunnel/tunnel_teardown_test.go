// Copyright 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tunnel

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc/peer"
)

// Regression tests for teardown paths that only misbehave under contention or
// with state that normal operation rarely leaves behind. Each fails on the
// code they were written against; the clientTargets one only under -race.

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// A second, concurrent Start must report "client is already running" and
// leave the instance that is actually running untouched. Start used to defer
// its cancel on every return path, so the spurious call cancelled the running
// instance's context and overwrote its error.
func TestClientStartSpuriousCallKeepsRunningInstance(t *testing.T) {
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:45000")
	if err != nil {
		t.Fatalf("failed to resolve: %v", err)
	}
	ctx, cancel := context.WithCancel(peer.NewContext(context.Background(), &peer.Peer{Addr: addr}))
	defer cancel()
	c, err := NewClient(&tunnelBlockingClient{}, ClientConfig{}, map[Target]struct{}{})
	if err != nil {
		t.Fatalf("failed to create new client: %v", err)
	}
	if err := c.Register(ctx); err != nil {
		t.Fatalf("c.Register() failed: %v", err)
	}

	done := make(chan struct{})
	go func() {
		c.Start(ctx)
		close(done)
	}()
	waitFor(t, "the first Start to hold the run token", func() bool { return len(c.block) == 1 })

	// The spurious call must return immediately with the error recorded.
	c.Start(ctx)
	if got := c.Error(); got == nil || got.Error() != "client is already running" {
		t.Fatalf("c.Error() after spurious Start = %v, want \"client is already running\"", got)
	}

	// ...and the running instance must be neither cancelled nor returned.
	select {
	case <-done:
		t.Fatal("spurious Start tore down the running instance")
	case <-time.After(200 * time.Millisecond):
	}
	c.emu.RLock()
	cancelled := c.cancelFunc == nil
	c.emu.RUnlock()
	if cancelled {
		t.Fatal("spurious Start cancelled the running instance's context")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("running instance did not stop on context cancellation")
	}
}
