# Data-Oriented Pipeline Plan

## Three Data Zones

| Zone | Backing | Lifetime | Purpose |
|------|---------|----------|---------|
| **Transient** | MMU-mirrored ring buffer | Until consumed by parser | Raw bytes off the wire |
| **Command lifecycle** | Arena allocator | One command execution, reset after response sent | Parsed RESP values, command args, response values |
| **Persistent** | GPA / Store | Until explicitly deleted or expired | Stored key-value data |

## Pipeline Flow

```
bytes (wire) → parsed RESP → command semantics → store mutation → response RESP → bytes (wire)
 [transient]     [arena]        [arena]           [persistent]     [arena]        [send buf]
  ring buf                                          GPA/Store
```

## Module Map

```
main.zig          thin wiring: create reactor + engine, run loop
reactor.zig       comptime-generic I/O multiplexer (poll/kqueue/io_uring)
network.zig       MMU ring buffer, Connection pool (fd + buffers + parser state)
protocol.zig      RespValue, streaming FSM parser, serializer
engine.zig        pure state transformer: command dispatch, blocked client lifecycle
storage.zig       Store (persistent zone, unchanged)
```

## Dependency Graph

```
main.zig
├── reactor.zig      (no deps)
├── network.zig      (depends on: protocol.Parser)
├── engine.zig       (depends on: protocol.RespValue, storage.Store)
├── protocol.zig     (no deps)
└── storage.zig      (no deps)
```

No circular dependencies. Single direction of data flow.

## One Iteration of the Main Loop

```
reactor.wait(engine.computeTimeout())      → events (readable fds, timeouts)

for each readable:
  network.recv(conn) into ring_buffer       → bytes in buffer
  while protocol.parse(ring_buf, arena)     → RespValue (zero-copy slices into ring buf)
    engine.execute(conn_id, cmd, arena)     → Effect
    dispatch effect:
      .reply         → serialize to send_buf
      .reply_and_wake → serialize to send_buf + resolve wake
      .block         → register blocked client in engine

for each writable:
  network.flush(conn)

expired = engine.expireBlocked()            → conn_ids to send null responses
arena.reset()
```

## Module Details

### reactor.zig — I/O Multiplexer

Pure I/O readiness notification. Knows nothing about connections, commands, or protocol.
Caller provides timeout (computed by engine from blocked client deadlines).

```zig
pub fn Reactor(comptime Backend: type) type {
    return struct {
        pub const Event = struct { fd: socket_t, readable: bool, writable: bool, err: bool };
        pub fn wait(self, timeout_ms: i32) ![]Event;
        pub fn register(self, fd, interests) !void;
        pub fn unregister(self, fd) !void;
    };
}

pub const PollBackend = struct { ... };
// Future: IoUringBackend, KqueueBackend
```

### network.zig — Ring Buffer + Connections

**MMU-mirrored ring buffer**: `memfd_create` + double `mmap` so `readableSlice()` is always
contiguous, even across wraparound. Enables zero-copy parser slices directly into the buffer.

**Connection**: I/O state only. No business logic, no blocked state.

```zig
pub const Connection = struct {
    fd: socket_t,
    recv_buf: RingBuffer,          // MMU-mirrored, incoming bytes
    send_buf: std.ArrayList(u8),   // growable, outgoing bytes
    parser: protocol.Parser,       // per-connection FSM state
};
```

Connection pool indexed by connection ID (not fd). Could use MultiArrayList for SoA if
iterating hot fields (fds) without touching cold fields (buffers) becomes worthwhile.

### protocol.zig — RESP Codec

**RespValue**: unchanged.

**Streaming FSM parser**: replaces recursive `parse()`. Explicit stack (max depth 8).
Returns slices into the ring buffer for bulk strings (zero-copy). Array allocations use arena.

```zig
pub const Parser = struct {
    state: State,
    stack: [8]Frame,
    depth: u3,

    pub fn feed(self: *Parser, data: []const u8, arena: Allocator) ?ParseResult;
    // Returns null if incomplete, ParseResult{value, consumed} if complete
};
```

**Serializer**: unchanged (writes into connection send_buf).

**Command enum**: stays here (RESP protocol semantics).

### engine.zig — Pure State Transformer

Takes `(command, store)`, returns `(Effect)`. No I/O, no fd awareness, no socket writes.
The main loop interprets effects.

```zig
pub const Engine = struct {
    store: *Store,
    blocked: BlockedMap,

    pub fn execute(self, conn_id: ConnId, command: RespValue, arena: Allocator) !Effect;
    pub fn computeTimeout(self) i32;
    pub fn expireBlocked(self) ![]ConnId;
};

pub const Effect = union(enum) {
    reply: RespValue,
    reply_and_wake: struct {
        reply: RespValue,
        wake_key: []const u8,
    },
    block: BlockInfo,
};
```

BlockInfo, BlockedOp, BlockedClient all live here (not in protocol.zig).
Wake resolution is also here but returns intents, not I/O:

```zig
pub fn resolveWake(self, key: []const u8, arena: Allocator) !?WakeResult;
// WakeResult = { conn_id, response } — main loop does the actual write
```

### storage.zig — Unchanged

Persistent zone. GPA. No changes needed for this refactor.

## Implementation Order

1. **network.zig** — ring buffer + Connection struct, testable in isolation
2. **reactor.zig** — extract PollBackend from main.zig
3. **protocol.zig** — rewrite parse() as streaming FSM, keep existing tests passing
4. **engine.zig** — extract handleCommand + server.zig, keep tests passing
5. **main.zig** — rewrite as thin loop wiring the modules together

Each step testable independently before moving to the next.
