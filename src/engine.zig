// src/engine.zig

const std = @import("std");
const posix = std.posix;
const protocol = @import("protocol.zig");
const storage = @import("storage.zig");
const RespValue = protocol.RespValue;

pub const BlockInfo = struct {
    timeout_ms: u64, // 0 is block forever
    keys: [][]const u8,
    operation: BlockedOp,
};

pub const BlockedOp = union(enum) {
    blpop: void,
    xread: struct { ids: []storage.RecordId }, // the start IDs per key
};

pub const BlockedClient = struct {
    deadline_ms: ?i64,
    keys: [][]const u8,
    operation: BlockedOp = .{ .blpop = {} },

    pub fn deinit(self: *BlockedClient, alloc: std.mem.Allocator) void {
        for (self.keys) |k| alloc.free(k);
        alloc.free(self.keys);
        switch (self.operation) {
            .xread => |x| alloc.free(x.ids),
            .blpop => {},
        }
    }
};

pub const Effect = union(enum) {
    block: BlockInfo,
    reply_and_wake: struct {
        reply: RespValue,
        wake_key: []const u8,
    },
    reply: RespValue,
};

pub const WakeResult = struct {
    fd: posix.socket_t,
    response: RespValue,
};

const Command = enum {
    PING,
    ECHO,
    SET,
    GET,
    INCR,
    RPUSH,
    LPUSH,
    LRANGE,
    LLEN,
    LPOP,
    BLPOP,
    TYPE,
    XADD,
    XRANGE,
    XREAD,

    pub fn parse(cmd_str: []const u8) ?Command {
        inline for (std.meta.fields(Command)) |f| {
            if (std.ascii.eqlIgnoreCase(cmd_str, f.name)) return @enumFromInt(f.value);
        }
        return null;
    }
};

/// Given a `RespValue`, parses and dispatches a `Command`.
/// Returns an `Effect` for the main loop to interpret.
pub fn execute(
    arena: std.mem.Allocator,
    store: *storage.Store,
    value: RespValue,
) !Effect {
    const items = switch (value) {
        .array => |arr| arr,
        else => return .{ .reply = .{ .error_msg = "empty array" } },
    };

    const cmd = items[0].bulk_string;
    const command = Command.parse(cmd) orelse return .{ .reply = .{ .error_msg = "unknown command" } };

    switch (command) {
        .PING => return .{ .reply = .{ .simple_string = "PONG" } },
        .ECHO => {
            if (wrongArgs(items, 2)) |r| return r;
            return .{ .reply = items[1] };
        },
        .SET => {
            if (wrongArgs(items, 3)) |r| return r;

            const key = items[1].bulk_string;
            const val = items[2].bulk_string;
            var expires_at: ?i64 = null;
            if (items.len >= 5) {
                const arg = items[3].bulk_string;
                if (std.ascii.eqlIgnoreCase(arg, "PX")) {
                    const ps_ms: i64 = std.fmt.parseInt(i64, items[4].bulk_string, 10) catch {
                        return error.ProtocolError;
                    };
                    if (ps_ms <= 0) {
                        return error.ProtocolError;
                    }
                    expires_at = std.time.milliTimestamp() + ps_ms;
                }
            }

            try store.set(key, val, expires_at);
            return .{ .reply = .{ .simple_string = "OK" } };
        },
        .GET => {
            if (wrongArgs(items, 2)) |r| return r;

            const key = items[1].bulk_string;
            const v = store.get(key) orelse return .{ .reply = .{ .null_value = {} } };
            return .{ .reply = .{ .bulk_string = v } };
        },
        .INCR => {
            if (wrongArgs(items, 2)) |r| return r;

            const key = items[1].bulk_string;
            const v = store.get(key) orelse return .{ .reply = .{ .null_value = {} } };
            const int_value: i64 = 1 + (std.fmt.parseInt(i64, v, 10) catch {
                return .{ .reply = .{ .null_value = {} } };
            });
            var buf: [21]u8 = undefined;
            const incr_str = try std.fmt.bufPrint(&buf, "{}", .{int_value});
            try store.set(key, incr_str, null);
            return .{ .reply = .{ .integer = int_value } };
        },
        .LPUSH, .RPUSH => |c| {
            if (wrongArgs(items, 3)) |r| return r;

            const key = items[1].bulk_string;
            var number_of_elements: usize = 0;
            for (items[2..]) |item| {
                number_of_elements = if (c == .LPUSH)
                    try store.push(key, item.bulk_string, .left)
                else
                    try store.push(key, item.bulk_string, .right);
            }

            return .{ .reply_and_wake = .{
                .reply = .{ .integer = @intCast(number_of_elements) },
                .wake_key = key,
            } };
        },
        .LRANGE => {
            if (wrongArgs(items, 4)) |r| return r;

            const key = items[1].bulk_string;
            const start = std.fmt.parseInt(i64, items[2].bulk_string, 10) catch {
                return .{ .reply = .{ .error_msg = "value is not an integer" } };
            };
            const stop = std.fmt.parseInt(i64, items[3].bulk_string, 10) catch {
                return .{ .reply = .{ .error_msg = "value is not an integer" } };
            };
            const items_slice = try store.lrange(arena, key, start, stop) orelse {
                const empty = try arena.alloc(RespValue, 0);
                return .{ .reply = .{ .array = empty } };
            };
            const arr = try RespValue.bulkStringArray(arena, items_slice);
            return .{ .reply = .{ .array = arr } };
        },
        .LLEN => {
            if (wrongArgs(items, 2)) |r| return r;

            const key = items[1].bulk_string;
            const len = store.llen(key);
            return .{ .reply = .{ .integer = @intCast(len) } };
        },
        .LPOP => {
            if (wrongArgs(items, 2)) |r| return r;

            const key = items[1].bulk_string;
            const count_arg: ?usize = if (items.len > 2)
                std.fmt.parseInt(usize, items[2].bulk_string, 10) catch {
                    return .{ .reply = .{ .error_msg = "value is not an integer" } };
                }
            else
                null;
            const popped = try store.lpop(arena, key, count_arg orelse 1) orelse {
                return .{ .reply = .{ .null_value = {} } };
            };
            if (count_arg == null) {
                return .{ .reply = .{ .bulk_string = popped[0] } };
            }
            const arr = try RespValue.bulkStringArray(arena, popped);
            return .{ .reply = .{ .array = arr } };
        },
        .BLPOP => {
            if (wrongArgs(items, 3)) |r| return r;

            const timeout_s = std.fmt.parseFloat(f64, items[items.len - 1].bulk_string) catch {
                return .{ .reply = .{ .error_msg = "timeout is not a float or out of range" } };
            };
            if (timeout_s < 0) return .{ .reply = .{ .error_msg = "timeout is negative" } };
            const timeout_ms: u64 = if (timeout_s == 0) 0 else @max(1, @as(u64, @intFromFloat(timeout_s * 1000)));
            const keys = try arena.alloc([]const u8, items.len - 2);
            for (items[1 .. items.len - 1], keys) |item, *key| key.* = item.bulk_string;
            for (keys) |key| {
                const popped = try store.lpop(arena, key, 1) orelse continue;
                const resp_items = try arena.alloc(RespValue, 2);
                resp_items[0] = .{ .bulk_string = key };
                resp_items[1] = .{ .bulk_string = popped[0] };
                return .{ .reply = .{ .array = resp_items } };
            }
            return .{ .block = .{ .keys = keys, .timeout_ms = timeout_ms, .operation = .{ .blpop = {} } } };
        },
        .TYPE => {
            if (wrongArgs(items, 2)) |r| return r;

            const key = items[1].bulk_string;
            return .{ .reply = .{ .simple_string = switch (store.typeOf(key)) {
                .string => "string",
                .list => "list",
                .stream => "stream",
                .none => "none",
            } } };
        },
        .XADD => {
            if (wrongArgs(items, 4)) |r| return r;

            const key = items[1].bulk_string;
            const id = items[2].bulk_string;
            const args = try arena.alloc([]const u8, items.len - 3);
            for (items[3..], args) |item, *arg| arg.* = item.bulk_string;
            const returned_id = store.xadd(key, id, args) catch |err| switch (err) {
                error.InvalidId => return .{ .reply = .{ .error_msg = "The ID specified in XADD is equal or smaller than the target stream top item" } },
                error.MinId => return .{ .reply = .{ .error_msg = "The ID specified in XADD must be greater than 0-0" } },
                else => return err,
            };
            const id_str = try returned_id.toStr(arena);
            return .{ .reply_and_wake = .{
                .reply = .{ .bulk_string = id_str },
                .wake_key = key,
            } };
        },
        .XRANGE => {
            if (wrongArgs(items, 4)) |r| return r;

            const key = items[1].bulk_string;
            const start_id = items[2].bulk_string;
            const end_id = items[3].bulk_string;
            const range_slice = store.streamQuery(key, start_id, end_id, false) orelse {
                return .{ .reply = .{ .array = try arena.alloc(RespValue, 0) } };
            };
            const range_array = try assembleStreamResp(arena, range_slice);
            return .{ .reply = .{ .array = range_array } };
        },
        .XREAD => {
            if (wrongArgs(items, 3)) |r| return r;

            var idx: usize = 1;
            var block_ms: ?u64 = null;

            while (idx < items.len) {
                const arg = items[idx].bulk_string;
                if (std.ascii.eqlIgnoreCase(arg, "BLOCK")) {
                    idx += 1;
                    block_ms = std.fmt.parseInt(u64, items[idx].bulk_string, 10) catch
                        return .{ .reply = .{ .error_msg = "timeout is not an integer" } };
                    idx += 1;
                } else if (std.ascii.eqlIgnoreCase(arg, "STREAMS")) {
                    idx += 1;
                    break;
                } else {
                    idx += 1;
                }
            }

            const rest = items[idx..];
            const mid = rest.len / 2;
            const keys = try arena.alloc([]const u8, mid);
            const ids = try arena.alloc([]const u8, mid);
            for (rest[0..mid], keys) |item, *k| k.* = item.bulk_string;
            for (rest[mid..], ids) |item, *id| id.* = item.bulk_string;

            // Query all streams
            var response: std.ArrayList(RespValue) = .empty;
            var has_results = false;
            for (keys, ids) |key_str, id_str| {
                const range_slice = store.streamQuery(key_str, id_str, "+", true) orelse continue;
                if (range_slice.len == 0) continue;
                has_results = true;
                const range_array = try assembleStreamResp(arena, range_slice);
                const key_entry = try arena.alloc(RespValue, 2);
                key_entry[0] = .{ .bulk_string = key_str };
                key_entry[1] = .{ .array = range_array };
                try response.append(arena, .{ .array = key_entry });
            }

            if (has_results) {
                return .{ .reply = .{ .array = try response.toOwnedSlice(arena) } };
            }

            if (block_ms) |timeout_ms| {
                const resolved_ids = try arena.alloc(storage.RecordId, ids.len);
                for (ids, keys, resolved_ids) |id_str, key_str, *out| {
                    if (std.ascii.eqlIgnoreCase(id_str, "$")) {
                        out.* = if (store.getStream(key_str)) |s| s.last_id else .{ .ms = 0, .sequence = 0 };
                    } else {
                        out.* = storage.RecordId.parseId(id_str) catch return .{ .reply = .{ .error_msg = "Invalid stream ID" } };
                    }
                }
                return .{ .block = .{
                    .keys = keys,
                    .timeout_ms = timeout_ms,
                    .operation = .{ .xread = .{ .ids = resolved_ids } },
                } };
            }
            return .{ .reply = .{ .null_value = {} } };
        },
    }
}

pub fn blockClient(
    blocked: *std.AutoHashMap(posix.socket_t, BlockedClient),
    alloc: std.mem.Allocator,
    fd: posix.socket_t,
    info: BlockInfo,
) !void {
    const keys = try alloc.alloc([]const u8, info.keys.len);
    for (info.keys, keys) |src, *dst| dst.* = try alloc.dupe(u8, src);
    const operation: BlockedOp = switch (info.operation) {
        .blpop => .{ .blpop = {} },
        .xread => |x| blk: {
            const ids = try alloc.alloc(storage.RecordId, x.ids.len);
            @memcpy(ids, x.ids);
            break :blk .{ .xread = .{ .ids = ids } };
        },
    };
    const deadline_ms: ?i64 = if (info.timeout_ms == 0) null else std.time.milliTimestamp() + @as(i64, @intCast(info.timeout_ms));
    try blocked.put(fd, .{ .keys = keys, .deadline_ms = deadline_ms, .operation = operation });
}

pub fn computeTimeout(blocked: *std.AutoHashMap(posix.socket_t, BlockedClient)) i32 {
    var poll_timeout_ms: i32 = -1;
    const now = std.time.milliTimestamp();
    var it = blocked.iterator();
    while (it.next()) |e| {
        if (e.value_ptr.deadline_ms) |dl| {
            const ms: i32 = @intCast(@max(0, @min(dl - now, std.math.maxInt(i32))));
            if (poll_timeout_ms == -1 or ms < poll_timeout_ms) poll_timeout_ms = ms;
        }
    }
    return poll_timeout_ms;
}

/// Returns list of expired fds. Removes them from blocked and calls deinit.
/// Caller is responsible for sending null responses to these fds.
pub fn expireBlocked(
    alloc: std.mem.Allocator,
    blocked: *std.AutoHashMap(posix.socket_t, BlockedClient),
) ![]posix.socket_t {
    const now_ms = std.time.milliTimestamp();
    var expired: std.ArrayList(posix.socket_t) = .empty;
    var it = blocked.iterator();
    while (it.next()) |e| {
        if (e.value_ptr.deadline_ms) |dl_ms|
            if (now_ms >= dl_ms) try expired.append(alloc, e.key_ptr.*);
    }
    for (expired.items) |fd| {
        var entry = blocked.fetchRemove(fd).?;
        entry.value.deinit(alloc);
    }
    return try expired.toOwnedSlice(alloc);
}

/// Find the first blocked client waiting on `key`, compute its response,
/// remove it from `blocked`, and return what to send.
pub fn resolveWake(
    key: []const u8,
    store: *storage.Store,
    blocked: *std.AutoHashMap(posix.socket_t, BlockedClient),
    server_alloc: std.mem.Allocator,
    arena: std.mem.Allocator,
) !?WakeResult {
    var it = blocked.iterator();

    while (it.next()) |e| {
        for (e.value_ptr.keys) |k| {
            if (!std.mem.eql(u8, k, key)) continue;

            const fd = e.key_ptr.*;
            var entry = blocked.fetchRemove(fd).?;
            defer entry.value.deinit(server_alloc);

            const resp: RespValue = switch (entry.value.operation) {
                .blpop => blk: {
                    const popped = try store.lpop(arena, key, 1) orelse return null;
                    const resp_items = try arena.alloc(RespValue, 2);
                    resp_items[0] = .{ .bulk_string = key };
                    resp_items[1] = .{ .bulk_string = popped[0] };
                    break :blk .{ .array = resp_items };
                },
                .xread => |r| blk: {
                    var response: std.ArrayList(RespValue) = .empty;
                    for (entry.value.keys, r.ids) |key_str, id| {
                        const range_slice = store.streamQueryFrom(key_str, id) orelse continue;
                        if (range_slice.len == 0) continue;
                        const range_array = try assembleStreamResp(arena, range_slice);
                        const key_entry = try arena.alloc(RespValue, 2);
                        // Dupe key_str into arena since entry.value.deinit will free the originals
                        key_entry[0] = .{ .bulk_string = try arena.dupe(u8, key_str) };
                        key_entry[1] = .{ .array = range_array };
                        try response.append(arena, .{ .array = key_entry });
                    }
                    break :blk .{ .array = try response.toOwnedSlice(arena) };
                },
            };

            return .{ .fd = fd, .response = resp };
        }
    }
    return null;
}

fn assembleStreamResp(alloc: std.mem.Allocator, stream_slice: []const storage.StreamRecord) ![]RespValue {
    const result = try alloc.alloc(RespValue, stream_slice.len);
    for (stream_slice, result) |record, *resp| {
        const id_str = try record.id.toStr(alloc);
        const fields = try alloc.alloc(RespValue, record.fields.len);
        for (record.fields, fields) |f, *r| r.* = .{ .bulk_string = f };

        const entry_arr = try alloc.alloc(RespValue, 2);
        entry_arr[0] = .{ .bulk_string = id_str };
        entry_arr[1] = .{ .array = fields };
        resp.* = .{ .array = entry_arr };
    }
    return result;
}

fn wrongArgs(items: []const RespValue, min: usize) ?Effect {
    if (items.len < min) return .{ .reply = .{ .error_msg = "wrong number of arguments" } };
    return null;
}

// Tests

const TestCtx = struct {
    store: storage.Store,
    arena: std.heap.ArenaAllocator,

    fn init() TestCtx {
        return .{
            .store = storage.Store.init(std.testing.allocator),
            .arena = std.heap.ArenaAllocator.init(std.testing.allocator),
        };
    }

    fn deinit(self: *TestCtx) void {
        self.store.deinit();
        self.arena.deinit();
    }

    fn cmd(self: *TestCtx, args: []const []const u8) ![]const u8 {
        const alloc = self.arena.allocator();
        const resp_args = try alloc.alloc(RespValue, args.len);
        for (args, resp_args) |arg, *resp| resp.* = .{ .bulk_string = arg };
        const effect = try execute(alloc, &self.store, .{ .array = resp_args });
        const resp = switch (effect) {
            .reply => |r| r,
            .reply_and_wake => |p| p.reply,
            .block => return error.UnexpectedBlock,
        };
        var a: std.io.Writer.Allocating = .init(alloc);
        try protocol.serialize(&a.writer, &resp);
        return try a.toOwnedSlice();
    }

    fn cmdEffect(self: *TestCtx, args: []const []const u8) !Effect {
        const alloc = self.arena.allocator();
        const resp_args = try alloc.alloc(RespValue, args.len);
        for (args, resp_args) |arg, *resp| resp.* = .{ .bulk_string = arg };
        return execute(alloc, &self.store, .{ .array = resp_args });
    }

    fn expect(self: *TestCtx, expected: []const u8, args: []const []const u8) !void {
        try std.testing.expectEqualStrings(expected, try self.cmd(args));
    }
};

test "execute: PING returns PONG" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    try ctx.expect("+PONG\r\n", &.{"PING"});
}

test "execute: ECHO replies" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    try ctx.expect("$5\r\nhello\r\n", &.{ "ECHO", "hello" });
}

test "execute: SET returns OK" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    try ctx.expect("+OK\r\n", &.{ "SET", "foo", "bar" });
}

test "execute: SET & GET returns key" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    _ = try ctx.cmd(&.{ "SET", "foo", "bar" });
    try ctx.expect("$3\r\nbar\r\n", &.{ "GET", "foo" });
}

test "execute: GET returns RESP null bulk string on non-existant string" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    try ctx.expect("$-1\r\n", &.{ "GET", "foo" });
}

test "execute: RPUSH creates a new list for new key" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    try ctx.expect(":1\r\n", &.{ "RPUSH", "foo", "bar" });
}

test "execute: RPUSH appends to list for existing key" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    _ = try ctx.cmd(&.{ "RPUSH", "foo", "bar" });
    try ctx.expect(":2\r\n", &.{ "RPUSH", "foo", "bash" });
}

test "execute: RPUSH handles multiple elements" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    try ctx.expect(":2\r\n", &.{ "RPUSH", "foo", "bar", "bash" });
    try ctx.expect(":3\r\n", &.{ "RPUSH", "foo", "titi" });
}

test "execute: LPUSH creates a new list for new key" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    try ctx.expect(":1\r\n", &.{ "LPUSH", "foo", "bar" });
}

test "execute: LPUSH appends to list for existing key" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    _ = try ctx.cmd(&.{ "LPUSH", "foo", "bar" });
    try ctx.expect(":2\r\n", &.{ "LPUSH", "foo", "bash" });
}

test "execute: LPUSH handles multiple elements" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    try ctx.expect(":2\r\n", &.{ "LPUSH", "foo", "bar", "bash" });
    try ctx.expect(":3\r\n", &.{ "LPUSH", "foo", "titi" });
}

test "execute: LRANGE returns elements" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    _ = try ctx.cmd(&.{ "RPUSH", "mylist", "one", "two", "three" });
    try ctx.expect("*3\r\n$3\r\none\r\n$3\r\ntwo\r\n$5\r\nthree\r\n", &.{ "LRANGE", "mylist", "0", "2" });
}

test "execute: LRANGE stop beyond end clamps to list length" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    _ = try ctx.cmd(&.{ "RPUSH", "mylist", "a", "b" });
    try ctx.expect("*2\r\n$1\r\na\r\n$1\r\nb\r\n", &.{ "LRANGE", "mylist", "0", "100" });
}

test "execute: LRANGE returns empty for non-existent key" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    try ctx.expect("*0\r\n", &.{ "LRANGE", "nokey", "0", "1" });
}

test "execute: LRANGE negative stop returns full list" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    _ = try ctx.cmd(&.{ "RPUSH", "mylist", "one", "two", "three" });
    try ctx.expect("*3\r\n$3\r\none\r\n$3\r\ntwo\r\n$5\r\nthree\r\n", &.{ "LRANGE", "mylist", "0", "-1" });
}

test "execute: LLEN returns length for existing list" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    _ = try ctx.cmd(&.{ "RPUSH", "list", "a", "b" });
    try ctx.expect(":2\r\n", &.{ "LLEN", "list" });
}

test "execute: LLEN returns 0 for non-existent list" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    try ctx.expect(":0\r\n", &.{ "LLEN", "list" });
}

test "execute: LPOP pops a single item from a list" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    _ = try ctx.cmd(&.{ "RPUSH", "list", "one", "two", "three", "four", "five" });
    try ctx.expect("$3\r\none\r\n", &.{ "LPOP", "list" });
}

test "execute: LPOP pops multiple items from a list" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    _ = try ctx.cmd(&.{ "RPUSH", "list", "one", "two", "three", "four", "five" });
    try ctx.expect("*2\r\n$3\r\none\r\n$3\r\ntwo\r\n", &.{ "LPOP", "list", "2" });
}

test "execute: LPOP returns null bulk string for empty list" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    _ = try ctx.cmd(&.{ "RPUSH", "list", "one" });
    _ = try ctx.cmd(&.{ "LPOP", "list" });
    try ctx.expect("$-1\r\n", &.{ "LPOP", "list" });
}

test "execute: LPOP returns null bulk string for non-existent list" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    _ = try ctx.cmd(&.{ "LPOP", "list" });
    try ctx.expect("$-1\r\n", &.{ "LPOP", "list" });
}

test "execute: BLPOP returns immediate response when list has data" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    _ = try ctx.cmd(&.{ "RPUSH", "mylist", "hello" });
    try ctx.expect("*2\r\n$6\r\nmylist\r\n$5\r\nhello\r\n", &.{ "BLPOP", "mylist", "0" });
}

test "execute: BLPOP checks multiple keys in order" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    _ = try ctx.cmd(&.{ "RPUSH", "b", "val" });
    try ctx.expect("*2\r\n$1\r\nb\r\n$3\r\nval\r\n", &.{ "BLPOP", "a", "b", "0" });
}

test "execute: BLPOP returns block effect on empty list" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    const effect = try ctx.cmdEffect(&.{ "BLPOP", "mylist", "5" });
    switch (effect) {
        .block => |b| {
            try std.testing.expectEqual(@as(usize, 1), b.keys.len);
            try std.testing.expectEqualStrings("mylist", b.keys[0]);
            try std.testing.expectEqual(@as(u64, 5000), b.timeout_ms);
        },
        else => return error.WrongEffect,
    }
}

test "execute: BLPOP timeout 0 means block forever" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    const effect = try ctx.cmdEffect(&.{ "BLPOP", "mylist", "0" });
    switch (effect) {
        .block => |b| try std.testing.expectEqual(@as(u64, 0), b.timeout_ms),
        else => return error.WrongEffect,
    }
}

test "execute: RPUSH returns reply_and_wake with correct key" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    const effect = try ctx.cmdEffect(&.{ "RPUSH", "mylist", "a" });
    switch (effect) {
        .reply_and_wake => |p| {
            try std.testing.expectEqualStrings("mylist", p.wake_key);
            const alloc = ctx.arena.allocator();
            var a: std.io.Writer.Allocating = .init(alloc);
            try protocol.serialize(&a.writer, &p.reply);
            const serialized = try a.toOwnedSlice();
            try std.testing.expectEqualStrings(":1\r\n", serialized);
        },
        else => return error.WrongEffect,
    }
}

test "execute: XADD creates stream and returns id" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    try ctx.expect("$3\r\n1-0\r\n", &.{ "XADD", "mystream", "1-0", "key", "value" });
}

test "execute: XADD auto-sequences within same ms" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    _ = try ctx.cmd(&.{ "XADD", "mystream", "1-0", "a", "1" });
    try ctx.expect("$3\r\n1-1\r\n", &.{ "XADD", "mystream", "1-*", "b", "2" });
}

test "execute: XADD rejects id equal or smaller than last" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    _ = try ctx.cmd(&.{ "XADD", "mystream", "2-0", "a", "1" });
    try ctx.expect("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n", &.{ "XADD", "mystream", "1-0", "b", "2" });
}

test "execute: XADD rejects 0-0" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    try ctx.expect("-ERR The ID specified in XADD must be greater than 0-0\r\n", &.{ "XADD", "mystream", "0-0", "a", "1" });
}

test "execute: XADD multiple entries" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    _ = try ctx.cmd(&.{ "XADD", "s", "1-0", "a", "1" });
    _ = try ctx.cmd(&.{ "XADD", "s", "2-0", "b", "2" });
    try ctx.expect("$3\r\n3-0\r\n", &.{ "XADD", "s", "3-0", "c", "3" });
}

test "execute: XRANGE returns empty for non-existent key" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    try ctx.expect("*0\r\n", &.{ "XRANGE", "nostream", "0", "99" });
}

test "execute: TYPE returns stream for stream key" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    _ = try ctx.cmd(&.{ "XADD", "mystream", "1-0", "k", "v" });
    try ctx.expect("+stream\r\n", &.{ "TYPE", "mystream" });
}

test "execute: XREAD single stream returns entries after id" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    _ = try ctx.cmd(&.{ "XADD", "s1", "1-0", "a", "1" });
    _ = try ctx.cmd(&.{ "XADD", "s1", "2-0", "b", "2" });
    const result = try ctx.cmd(&.{ "XREAD", "STREAMS", "s1", "0-0" });
    try std.testing.expect(std.mem.indexOf(u8, result, "s1") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "1-0") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "2-0") != null);
}

test "execute: XREAD returns null for non-existent stream" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    try ctx.expect("$-1\r\n", &.{ "XREAD", "STREAMS", "nostream", "0-0" });
}

test "execute: XREAD multiple streams" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    _ = try ctx.cmd(&.{ "XADD", "s1", "1-0", "a", "1" });
    _ = try ctx.cmd(&.{ "XADD", "s2", "5-0", "x", "y" });
    const result = try ctx.cmd(&.{ "XREAD", "STREAMS", "s1", "s2", "0-0", "0-0" });
    try std.testing.expect(std.mem.indexOf(u8, result, "s1") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "s2") != null);
}

test "execute: XREAD exclusive start skips exact id match" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    _ = try ctx.cmd(&.{ "XADD", "s1", "1-0", "a", "1" });
    _ = try ctx.cmd(&.{ "XADD", "s1", "2-0", "b", "2" });
    const result = try ctx.cmd(&.{ "XREAD", "STREAMS", "s1", "1-0" });
    try std.testing.expect(std.mem.indexOf(u8, result, "2-0") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "1-0\r\n") == null);
}

test "execute: XREAD with BLOCK blocks when only matching entry is the start id" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    _ = try ctx.cmd(&.{ "XADD", "s1", "0-1", "k", "v" });
    const effect = try ctx.cmdEffect(&.{ "XREAD", "BLOCK", "1000", "STREAMS", "s1", "0-1" });
    switch (effect) {
        .block => {},
        else => return error.WrongEffect,
    }
}

test "execute: XREAD with BLOCK returns block effect when no data" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    const effect = try ctx.cmdEffect(&.{ "XREAD", "BLOCK", "5000", "STREAMS", "s1", "0-0" });
    switch (effect) {
        .block => |b| {
            try std.testing.expectEqual(@as(usize, 1), b.keys.len);
            try std.testing.expectEqualStrings("s1", b.keys[0]);
            try std.testing.expectEqual(@as(u64, 5000), b.timeout_ms);
            switch (b.operation) {
                .xread => |x| {
                    try std.testing.expectEqual(@as(usize, 1), x.ids.len);
                    try std.testing.expectEqualStrings("0-0", x.ids[0]);
                },
                else => return error.WrongOperation,
            }
        },
        else => return error.WrongEffect,
    }
}

test "execute: XREAD with BLOCK returns data immediately if available" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    _ = try ctx.cmd(&.{ "XADD", "s1", "1-0", "k", "v" });
    const result = try ctx.cmd(&.{ "XREAD", "BLOCK", "0", "STREAMS", "s1", "0-0" });
    try std.testing.expect(std.mem.indexOf(u8, result, "1-0") != null);
}

test "execute: XADD returns reply_and_wake with correct key" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    const effect = try ctx.cmdEffect(&.{ "XADD", "mystream", "1-0", "k", "v" });
    switch (effect) {
        .reply_and_wake => |w| try std.testing.expectEqualStrings("mystream", w.wake_key),
        else => return error.WrongEffect,
    }
}

test "computeTimeout: returns -1 with no blocked clients" {
    var blocked = std.AutoHashMap(posix.socket_t, BlockedClient).init(std.testing.allocator);
    defer blocked.deinit();
    try std.testing.expectEqual(@as(i32, -1), computeTimeout(&blocked));
}

test "computeTimeout: returns -1 when deadline is null (block forever)" {
    var blocked = std.AutoHashMap(posix.socket_t, BlockedClient).init(std.testing.allocator);
    defer blocked.deinit();
    const keys = try std.testing.allocator.alloc([]const u8, 1);
    keys[0] = try std.testing.allocator.dupe(u8, "k");
    try blocked.put(10, .{ .keys = keys, .deadline_ms = null });
    defer {
        var entry = blocked.fetchRemove(10).?;
        entry.value.deinit(std.testing.allocator);
    }
    try std.testing.expectEqual(@as(i32, -1), computeTimeout(&blocked));
}

test "resolveWake: blpop wakes blocked client and returns response" {
    var store = storage.Store.init(std.testing.allocator);
    defer store.deinit();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var blocked = std.AutoHashMap(posix.socket_t, BlockedClient).init(std.testing.allocator);
    defer blocked.deinit();

    _ = try store.push("mylist", "hello", .right);

    const keys = try std.testing.allocator.alloc([]const u8, 1);
    keys[0] = try std.testing.allocator.dupe(u8, "mylist");
    try blocked.put(42, .{ .keys = keys, .deadline_ms = null });

    const result = try resolveWake("mylist", &store, &blocked, std.testing.allocator, arena.allocator());
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(posix.socket_t, 42), result.?.fd);
    try std.testing.expectEqual(@as(u32, 0), blocked.count());

    const arr = result.?.response.array;
    try std.testing.expectEqual(@as(usize, 2), arr.len);
    try std.testing.expectEqualStrings("mylist", arr[0].bulk_string);
    try std.testing.expectEqualStrings("hello", arr[1].bulk_string);
}

test "resolveWake: xread wakes blocked client with new stream entries" {
    var store = storage.Store.init(std.testing.allocator);
    defer store.deinit();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var blocked = std.AutoHashMap(posix.socket_t, BlockedClient).init(std.testing.allocator);
    defer blocked.deinit();

    var fields = [_][]const u8{ "temp", "42" };
    _ = try store.xadd("mystream", "1-0", &fields);

    const keys = try std.testing.allocator.alloc([]const u8, 1);
    keys[0] = try std.testing.allocator.dupe(u8, "mystream");
    const ids = try std.testing.allocator.alloc([]const u8, 1);
    ids[0] = try std.testing.allocator.dupe(u8, "0-0");
    try blocked.put(7, .{
        .keys = keys,
        .deadline_ms = null,
        .operation = .{ .xread = .{ .ids = ids } },
    });

    const result = try resolveWake("mystream", &store, &blocked, std.testing.allocator, arena.allocator());
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(posix.socket_t, 7), result.?.fd);
    try std.testing.expectEqual(@as(u32, 0), blocked.count());

    const outer = result.?.response.array;
    try std.testing.expectEqual(@as(usize, 1), outer.len);
    const stream_entry = outer[0].array;
    try std.testing.expectEqualStrings("mystream", stream_entry[0].bulk_string);
}

test "resolveWake: returns null when no blocked client matches key" {
    var store = storage.Store.init(std.testing.allocator);
    defer store.deinit();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var blocked = std.AutoHashMap(posix.socket_t, BlockedClient).init(std.testing.allocator);
    defer blocked.deinit();

    const result = try resolveWake("nokey", &store, &blocked, std.testing.allocator, arena.allocator());
    try std.testing.expect(result == null);
}

test "resolveWake: only wakes first matching client (FIFO)" {
    var store = storage.Store.init(std.testing.allocator);
    defer store.deinit();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var blocked = std.AutoHashMap(posix.socket_t, BlockedClient).init(std.testing.allocator);
    defer {
        var it = blocked.valueIterator();
        while (it.next()) |b| b.deinit(std.testing.allocator);
        blocked.deinit();
    }

    _ = try store.push("q", "val1", .right);
    _ = try store.push("q", "val2", .right);

    for ([_]posix.socket_t{ 10, 20 }) |fd| {
        const keys = try std.testing.allocator.alloc([]const u8, 1);
        keys[0] = try std.testing.allocator.dupe(u8, "q");
        try blocked.put(fd, .{ .keys = keys, .deadline_ms = null });
    }

    const r1 = try resolveWake("q", &store, &blocked, std.testing.allocator, arena.allocator());
    try std.testing.expect(r1 != null);
    try std.testing.expectEqual(@as(u32, 1), blocked.count());
}
