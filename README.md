# Concurrent Go RPC chat

A complete terminal chat application using the standard library only. The
server and each client run as separate programs. Communication uses TCP and
Go `net/rpc`, with polling (not callbacks).

## Run

Install Go 1.20 or newer. Open a terminal in this folder:

```sh
go run ./cmd/server
```

In two other terminals, from the same folder:

```sh
go run ./cmd/client -name Alice
```

```sh
go run ./cmd/client -name Bob
```

Type any message and press Enter. Other connected users receive it automatically
within roughly 500 ms. The sender receives no broadcast copy (the terminal already
echoes typed input).

- `/users`: show the alphabetically sorted list, including yourself.
- `/help`: show commands.
- `/quit`: disconnect gracefully.
- Ctrl+C or stdin EOF: disconnect and exit.

Usernames are case-sensitive, unique, and restricted to 1–24 ASCII letters,
digits or underscores. Duplicate names are rejected; rerun with another name.
Messages are limited to 4096 bytes; terminal control characters are rejected.

Build standalone executables if preferred (use `.exe` suffixes on Windows):

```sh
go build -o server ./cmd/server
go build -o client ./cmd/client
./server
# Separate terminals:
./client -name Alice
./client -name Bob
```

For a trusted LAN, listen on all interfaces and connect to the server's IP:

```sh
go run ./cmd/server -addr 0.0.0.0:9000
go run ./cmd/client -addr 192.168.1.10:9000 -name Alice
```

Allow TCP port 9000 through the server's firewall. The default binds only to
loopback. This classroom implementation has no TLS, account authentication,
persistent history or reconnect/retry mechanism; use it locally or on a trusted
network, not as a public internet service.

## Architecture

| File | Responsibility |
| --- | --- |
| `chat/chat.go` | Shared RPC types, user registry, per-user queues, synchronization and sessions |
| `cmd/server/main.go` | Listener, connection goroutines, signal handling and shutdown |
| `cmd/client/main.go` | Terminal input, polling, RPC timeouts and display |
| `chat/chat_test.go` | Real RPC tests over in-memory connections |

Each accepted connection gets a separate `rpc.Server` with a connection-bound
`Chat` receiver. The shared hub holds users and pending events. Clients cannot
choose another sender's username in a Send request. `net/rpc` dispatches calls
concurrently; the hub mutex protects every registry and queue operation.
No blocking RPC/network message delivery happens while broadcasting: broadcasting
only appends to queues. Poll detaches its queue before unlocking, so encoding a
reply cannot race with later broadcasts. A single lock establishes event order.

RPC methods:

| Method | Request | Reply / behavior |
| --- | --- | --- |
| `Chat.Join` | `JoinArgs{Username}` | Reserve name; notify others |
| `Chat.Send` | `SendArgs{Text}` | Queue message for everyone except sender |
| `Chat.Poll` | `Empty{}` | `PollReply{Events, Dropped}`; drain queue and refresh deadline |
| `Chat.List` | `Empty{}` | `UsersReply{Users}` |
| `Chat.Leave` | `Empty{}` | Remove user and notify others once |

A client input goroutine reads stdin. The main event loop handles input and a
poll ticker and owns all output. Incoming messages display without pressing
Enter. This is a basic line-oriented interface: incoming output can visually
interrupt a partially typed line; it does not provide readline-style redraw.

## Disconnections and shutdown

- `/quit`, EOF and Ctrl+C attempt Leave with a one-second deadline, then close
  the connection. A signal interrupting an active RPC may close it first; server
  connection cleanup still sends the leave notification.
- A process crash or abrupt TCP close ends ServeConn and removes the user.
- Silent network failures and clients that stop polling expire after 15 seconds
  without a valid activity RPC. Poll, Send and List refresh the connection's
  read/write deadline. Unjoined connections expire too. Server timeout is
  configurable using `-timeout 15s`; client polling uses `-poll 500ms`. Keep
  polling comfortably below the timeout.
- Cleanup is idempotent: explicit Leave plus socket closure generates only one
  leave event. Names become reusable after cleanup.
- Client RPC calls time out after three seconds and close the connection.
  Transport failure exits the client; restart it to reconnect.
- Server Ctrl+C/SIGTERM closes the listener and every accepted connection, then
  waits for connection handlers. Clients report the lost connection and exit
  when their next RPC fails. No final shutdown event is guaranteed.

## Delivery semantics

Events exist only in memory and only for currently joined recipients. Successful
polling removes returned events. If a reply is lost after removal, those events
are lost: there are no acknowledgments or exactly-once delivery guarantees.
Send is never automatically retried because a lost reply can leave its result
uncertain. Queues hold at most 1024 events per user, dropping the oldest on
excess traffic and reporting the count on the next poll. This bounds backlog
memory per user; it is not a production resource-abuse defense.

## Verification

```sh
go test ./...
go test -race ./...
go vet ./...
```

The race detector needs a supported platform and a C compiler. Tests cover
join/leave notifications, duplicate usernames, unjoined calls, sender exclusion,
sorted user lists, username reuse, abrupt disconnects, idle expiry, concurrent
send/list operations, queue overflow, invalid input and hub shutdown.

Manual acceptance checks:

1. Start the server and Alice; start Bob. Alice sees Bob join.
2. Send from Alice. Bob sees it without entering anything; Alice gets no RPC echo.
3. Run `/users`; both names appear. Try a third client named Alice: rejected.
4. Run `/quit` in Bob. Alice sees one leave notice. Reconnect Bob.
5. Force-kill Bob. Alice sees a disconnect notice; silent failures may take 15 s.
6. Press Ctrl+C in a client, then in the server. Verify surviving clients exit.

### Verified in this build

Validated with Go 1.22.2 on Linux: `go test -race ./...`, `go vet ./...`, and
both executable builds passed. An additional TCP process test launched the
server and multiple clients and verified automatic display, sender exclusion,
duplicate rejection, user listing, forced process termination, username reuse,
client Ctrl+C and server Ctrl+C. VCS stamping was disabled for vet/build in the
generation environment because its surrounding workspace has inaccessible Git
metadata; this is not needed in an ordinary extracted project directory.
