// Package chat implements a connection-bound, polling RPC chat service.
package chat

import (
	"errors"
	"net"
	"net/rpc"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
)

const MaxMessage = 4096
const MaxPending = 1024

var usernamePattern = regexp.MustCompile(`^[A-Za-z0-9_]{1,24}$`)

type Empty struct{}
type JoinArgs struct{ Username string }
type SendArgs struct{ Text string }
type Event struct {
	Kind, Username, Text string
	Time                 time.Time
}
type PollReply struct {
	Events  []Event
	Dropped int
}
type UsersReply struct{ Users []string }

// All mutable session and hub state is protected by Hub.mu.
type Hub struct {
	mu          sync.Mutex
	users       map[string]*Session
	connections map[*Session]bool
	timeout     time.Duration
	closed      bool
}

type Session struct {
	hub     *Hub
	conn    net.Conn
	name    string
	pending []Event
	dropped int
	ended   bool
}

func NewHub(timeout time.Duration) *Hub {
	return &Hub{users: make(map[string]*Session), connections: make(map[*Session]bool), timeout: timeout}
}

// Serve runs in one goroutine per accepted connection. Each connection gets
// its own RPC receiver, preventing a client from acting as another username.
func (h *Hub) Serve(conn net.Conn) {
	s := &Session{hub: h, conn: conn}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		conn.Close()
		return
	}
	h.connections[s] = true
	conn.SetDeadline(time.Now().Add(h.timeout))
	h.mu.Unlock()
	defer func() {
		conn.Close()
		h.mu.Lock()
		s.removeLocked("disconnected")
		delete(h.connections, s)
		h.mu.Unlock()
	}()
	server := rpc.NewServer()
	if err := server.RegisterName("Chat", s); err != nil {
		return
	}
	server.ServeConn(conn)
}

// Close interrupts reads/writes, including clients that never joined.
func (h *Hub) Close() {
	h.mu.Lock()
	h.closed = true
	conns := make([]net.Conn, 0, len(h.connections))
	for s := range h.connections {
		conns = append(conns, s.conn)
	}
	h.mu.Unlock()
	for _, c := range conns {
		c.Close()
	}
}

func (s *Session) activeLocked() error {
	if s.hub.closed || s.ended {
		return errors.New("session closed")
	}
	if s.name == "" {
		return errors.New("join first")
	}
	return s.conn.SetDeadline(time.Now().Add(s.hub.timeout))
}

func (h *Hub) broadcastLocked(except *Session, event Event) {
	for _, target := range h.users {
		if target == except {
			continue
		}
		if len(target.pending) == MaxPending {
			copy(target.pending, target.pending[1:])
			target.pending = target.pending[:MaxPending-1]
			target.dropped++
		}
		target.pending = append(target.pending, event)
	}
}

func (s *Session) removeLocked(reason string) {
	if s.ended {
		return
	}
	s.ended = true
	if s.name == "" {
		return
	}
	delete(s.hub.users, s.name)
	s.pending = nil
	s.hub.broadcastLocked(s, Event{Kind: "leave", Username: s.name, Text: reason, Time: time.Now()})
}

func (s *Session) Join(args JoinArgs, reply *Empty) error {
	h := s.hub
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed || s.ended {
		return errors.New("session closed")
	}
	if s.name != "" {
		return errors.New("already joined")
	}
	name := strings.TrimSpace(args.Username)
	if !usernamePattern.MatchString(name) {
		return errors.New("username must be 1-24 letters, digits or underscores")
	}
	if _, exists := h.users[name]; exists {
		return errors.New("username already in use")
	}
	if err := s.conn.SetDeadline(time.Now().Add(h.timeout)); err != nil {
		return err
	}
	s.name = name
	h.users[name] = s
	h.broadcastLocked(s, Event{Kind: "join", Username: name, Time: time.Now()})
	return nil
}

func (s *Session) Send(args SendArgs, reply *Empty) error {
	h := s.hub
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := s.activeLocked(); err != nil {
		return err
	}
	text := strings.TrimSpace(args.Text)
	if text == "" || len(text) > MaxMessage {
		return errors.New("message must contain 1-4096 bytes")
	}
	// Prevent terminal control sequences and multiline notification spoofing.
	for _, r := range text {
		if unicode.IsControl(r) {
			return errors.New("control characters are not allowed")
		}
	}
	h.broadcastLocked(s, Event{Kind: "message", Username: s.name, Text: text, Time: time.Now()})
	return nil
}

func (s *Session) Poll(args Empty, reply *PollReply) error {
	h := s.hub
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := s.activeLocked(); err != nil {
		return err
	}
	reply.Events = s.pending
	reply.Dropped = s.dropped
	// Detach the backing array: RPC encodes the reply after this lock is released.
	s.pending = nil
	s.dropped = 0
	return nil
}

func (s *Session) List(args Empty, reply *UsersReply) error {
	h := s.hub
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := s.activeLocked(); err != nil {
		return err
	}
	for name := range h.users {
		reply.Users = append(reply.Users, name)
	}
	sort.Strings(reply.Users)
	return nil
}

func (s *Session) Leave(args Empty, reply *Empty) error {
	h := s.hub
	h.mu.Lock()
	defer h.mu.Unlock()
	s.removeLocked("left")
	return nil
}
