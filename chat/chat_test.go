package chat

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"testing"
	"time"
)

func connect(t *testing.T, h *Hub) *rpc.Client {
	t.Helper()
	server, client := net.Pipe()
	go h.Serve(server)
	c := rpc.NewClient(client)
	t.Cleanup(func() { c.Close() })
	return c
}
func mustCall(t *testing.T, c *rpc.Client, method string, args, reply interface{}) {
	t.Helper()
	done := c.Go("Chat."+method, args, reply, make(chan *rpc.Call, 1))
	select {
	case result := <-done.Done:
		if result.Error != nil {
			t.Fatal(result.Error)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("RPC timed out")
	}
}
func join(t *testing.T, h *Hub, name string) *rpc.Client {
	t.Helper()
	c := connect(t, h)
	mustCall(t, c, "Join", JoinArgs{name}, &Empty{})
	return c
}
func poll(t *testing.T, c *rpc.Client) PollReply {
	t.Helper()
	var r PollReply
	mustCall(t, c, "Poll", Empty{}, &r)
	return r
}

func TestJoinBroadcastListAndLeave(t *testing.T) {
	h := NewHub(5 * time.Second)
	defer h.Close()
	a := join(t, h, "Alice")
	b := join(t, h, "Bob")
	events := poll(t, a).Events
	if len(events) != 1 || events[0].Kind != "join" || events[0].Username != "Bob" {
		t.Fatalf("join: %+v", events)
	}
	if len(poll(t, b).Events) != 0 {
		t.Fatal("self join notification")
	}
	duplicate := connect(t, h)
	if err := duplicate.Call("Chat.Join", JoinArgs{"Alice"}, &Empty{}); err == nil {
		t.Fatal("duplicate accepted")
	}
	if err := duplicate.Call("Chat.Send", SendArgs{"unjoined"}, &Empty{}); err == nil {
		t.Fatal("unjoined sender accepted")
	}
	mustCall(t, a, "Send", SendArgs{"hello"}, &Empty{})
	if len(poll(t, a).Events) != 0 {
		t.Fatal("sender received own message")
	}
	events = poll(t, b).Events
	if len(events) != 1 || events[0].Text != "hello" || events[0].Username != "Alice" {
		t.Fatalf("message: %+v", events)
	}
	var users UsersReply
	mustCall(t, a, "List", Empty{}, &users)
	if fmt.Sprint(users.Users) != "[Alice Bob]" {
		t.Fatal(users.Users)
	}
	mustCall(t, b, "Leave", Empty{}, &Empty{})
	b.Close()
	events = poll(t, a).Events
	if len(events) != 1 || events[0].Kind != "leave" {
		t.Fatalf("leave: %+v", events)
	}
	// A departed client's late connection cleanup must not remove a new session.
	replacement := join(t, h, "Bob")
	if len(poll(t, replacement).Events) != 0 {
		t.Fatal("new session inherited queue")
	}
}

func TestAbruptDisconnectAndIdleTimeout(t *testing.T) {
	for _, idle := range []bool{false, true} {
		t.Run(fmt.Sprint(idle), func(t *testing.T) {
			h := NewHub(180 * time.Millisecond)
			defer h.Close()
			a := join(t, h, "Watcher")
			b := join(t, h, "Gone")
			poll(t, a)
			if !idle {
				b.Close()
			}
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				events := poll(t, a).Events
				for _, e := range events {
					if e.Kind == "leave" && e.Username == "Gone" {
						join(t, h, "Gone")
						return
					}
				}
				time.Sleep(10 * time.Millisecond)
			}
			t.Fatal("missing disconnect notification")
		})
	}
}

func TestConcurrentRPC(t *testing.T) {
	h := NewHub(10 * time.Second)
	defer h.Close()
	const n = 12
	clients := make([]*rpc.Client, n)
	for i := range clients {
		clients[i] = join(t, h, fmt.Sprintf("user%d", i))
	}
	for _, c := range clients {
		poll(t, c)
	}
	var wg sync.WaitGroup
	for i, c := range clients {
		wg.Add(1)
		go func(i int, c *rpc.Client) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				if err := c.Call("Chat.Send", SendArgs{fmt.Sprintf("%d:%d", i, j)}, &Empty{}); err != nil {
					t.Error(err)
				}
				var r UsersReply
				if err := c.Call("Chat.List", Empty{}, &r); err != nil {
					t.Error(err)
				}
			}
		}(i, c)
	}
	wg.Wait()
	for i, c := range clients {
		r := poll(t, c)
		if len(r.Events) != (n-1)*10 {
			t.Fatalf("got %d messages", len(r.Events))
		}
		for _, e := range r.Events {
			if e.Username == fmt.Sprintf("user%d", i) {
				t.Fatal("self message")
			}
		}
	}
}

func TestOverflowValidationAndShutdown(t *testing.T) {
	h := NewHub(5 * time.Second)
	defer h.Close()
	a := join(t, h, "Sender")
	b := join(t, h, "Slow")
	for _, text := range []string{"", "\x1b[2J"} {
		if err := a.Call("Chat.Send", SendArgs{text}, &Empty{}); err == nil {
			t.Fatal("invalid message accepted")
		}
	}
	for i := 0; i < MaxPending+3; i++ {
		mustCall(t, a, "Send", SendArgs{"message"}, &Empty{})
	}
	r := poll(t, b)
	if len(r.Events) != MaxPending || r.Dropped != 3 {
		t.Fatalf("overflow: %d/%d", len(r.Events), r.Dropped)
	}
	if len(poll(t, b).Events) != 0 {
		t.Fatal("queue not drained")
	}
	h.Close()
	if err := a.Call("Chat.List", Empty{}, &UsersReply{}); err == nil {
		t.Fatal("server still open")
	}
}

func TestPollWhileBroadcasting(t *testing.T) {
	h := NewHub(5 * time.Second)
	defer h.Close()
	a := join(t, h, "A")
	b := join(t, h, "B")
	poll(t, a)
	const count = 200
	done := make(chan error, 1)
	go func() {
		for i := 0; i < count; i++ {
			if err := a.Call("Chat.Send", SendArgs{"live"}, &Empty{}); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	received := 0
	deadline := time.Now().Add(3 * time.Second)
	for received < count && time.Now().Before(deadline) {
		received += len(poll(t, b).Events)
	}
	if received != count {
		t.Fatalf("received %d", received)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
