// src/protocol.zig

const std = @import("std");
const storage = @import("storage.zig");

pub const BlockInfo = struct {
    keys: [][]const u8,
    timeout_ms: u64, // 0 is block forever
};

pub const PushInfo = struct {
    response: RespValue,
    key: []const u8, // key that was pushed to
};

pub const Action = union(enum) {
    response: RespValue,
    block: BlockInfo,
    push: PushInfo,
};

pub const RespValue = union(enum) {
    simple_string: []const u8,
    bulk_string: []const u8,
    integer: i64,
    array: []RespValue,
    null_value,
    error_msg: []const u8,
};

const ParseResult = struct {
    consumed: usize,
    value: RespValue,
};

// Define as enum for switch in handleCommand()
const Command = enum {
    PING,
    ECHO,
    SET,
    GET,
    RPUSH,
    LPUSH,
    LRANGE,
    LLEN,
    LPOP,
    BLPOP,
    TYPE,
    XADD,
    XRANGE,
};

// Handles the action taken for each RESP command
// returns the Action containing RespValue to be serialized at write boundary
pub fn handleCommand(
    alloc: std.mem.Allocator,
    store: *storage.Store,
    value: RespValue,
) !Action {
    const items = switch (value) {
        .array => |arr| arr,
        else => return .{ .response = .{ .error_msg = "empty array" } },
    };

    const cmd = items[0].bulk_string;
    const upper = try std.ascii.allocUpperString(alloc, cmd);
    const command = std.meta.stringToEnum(Command, upper) orelse return .{ .response = .{ .error_msg = "unknown command" } };

    switch (command) {
        .PING => return .{ .response = .{ .simple_string = "PONG" } },
        .ECHO => {
            if (wrongArgs(items, 2)) |r| return r;
            return .{ .response = items[1] };
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
            return .{ .response = .{ .simple_string = "OK" } };
        },
        .GET => {
            if (wrongArgs(items, 2)) |r| return r;

            const key = items[1].bulk_string;
            const v = store.get(key) orelse return .{ .response = .{ .null_value = {} } };
            return .{ .response = .{ .bulk_string = v } };
        },
        .LPUSH, .RPUSH => |c| {
            if (wrongArgs(items, 3)) |r| return r;

            const key = items[1].bulk_string;
            var number_of_elements: usize = 0;
            for (items[2..]) |item| {
                number_of_elements = if (c == .LPUSH)
                    try store.lpush(key, item.bulk_string)
                else
                    try store.rpush(key, item.bulk_string);
            }

            return .{ .push = .{ .response = .{ .integer = @intCast(number_of_elements) }, .key = key } };
        },
        .LRANGE => {
            if (wrongArgs(items, 4)) |r| return r;

            const key = items[1].bulk_string;
            const start = std.fmt.parseInt(i64, items[2].bulk_string, 10) catch {
                return .{ .response = .{ .error_msg = "value is not an integer" } };
            };
            const stop = std.fmt.parseInt(i64, items[3].bulk_string, 10) catch {
                return .{ .response = .{ .error_msg = "value is not an integer" } };
            };
            const items_slice = try store.lrange(alloc, key, start, stop) orelse {
                const empty = try alloc.alloc(RespValue, 0);
                return .{ .response = .{ .array = empty } };
            };
            const arr = try alloc.alloc(RespValue, items_slice.len);
            for (items_slice, arr) |item, *resp| resp.* = .{ .bulk_string = item };
            return .{ .response = .{ .array = arr } };
        },
        .LLEN => {
            if (wrongArgs(items, 2)) |r| return r;

            const key = items[1].bulk_string;
            const len = store.llen(key);
            return .{ .response = .{ .integer = @intCast(len) } };
        },
        .LPOP => {
            if (wrongArgs(items, 2)) |r| return r;

            const key = items[1].bulk_string;
            const count_arg: ?usize = if (items.len > 2)
                std.fmt.parseInt(usize, items[2].bulk_string, 10) catch {
                    return .{ .response = .{ .error_msg = "value is not an integer" } };
                }
            else
                null;
            const popped = try store.lpop(alloc, key, count_arg orelse 1) orelse {
                return .{ .response = .{ .null_value = {} } };
            };
            if (count_arg == null) {
                return .{ .response = .{ .bulk_string = popped[0] } };
            }
            const arr = try alloc.alloc(RespValue, popped.len);
            for (popped, arr) |item, *resp| resp.* = .{ .bulk_string = item };
            return .{ .response = .{ .array = arr } };
        },
        .BLPOP => {
            if (wrongArgs(items, 3)) |r| return r;

            const timeout_s = std.fmt.parseFloat(f64, items[items.len - 1].bulk_string) catch {
                return .{ .response = .{ .error_msg = "timeout is not a float or out of range" } };
            };
            if (timeout_s < 0) return .{ .response = .{ .error_msg = "timeout is negative" } };
            const timeout_ms: u64 = if (timeout_s == 0) 0 else @max(1, @as(u64, @intFromFloat(timeout_s * 1000)));
            const keys = try alloc.alloc([]const u8, items.len - 2);
            for (items[1 .. items.len - 1], keys) |item, *key| key.* = item.bulk_string;
            for (keys) |key| {
                const popped = try store.lpop(alloc, key, 1) orelse continue;
                const resp_items = try alloc.alloc(RespValue, 2);
                resp_items[0] = .{ .bulk_string = key };
                resp_items[1] = .{ .bulk_string = popped[0] };
                return .{ .response = .{ .array = resp_items } };
            }
            return .{ .block = .{ .keys = keys, .timeout_ms = timeout_ms } };
        },
        .TYPE => {
            if (wrongArgs(items, 2)) |r| return r;

            const key = items[1].bulk_string;
            return .{ .response = .{ .simple_string = switch (store.typeOf(key)) {
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
            const args = try alloc.alloc([]const u8, items.len - 3);
            for (items[3..], args) |item, *arg| arg.* = item.bulk_string;
            const returned_id = store.xadd(key, id, args) catch |err| switch (err) {
                error.InvalidId => return .{ .response = .{ .error_msg = "The ID specified in XADD is equal or smaller than the target stream top item" } },
                error.MinId => return .{ .response = .{ .error_msg = "The ID specified in XADD must be greater than 0-0" } },
                else => return err,
            };
            const id_str = try std.fmt.allocPrint(
                alloc,
                "{d}-{d}",
                .{ returned_id.ms, returned_id.sequence },
            );
            return .{ .response = .{ .bulk_string = id_str } };
        },
        .XRANGE => {
            if (wrongArgs(items, 4)) |r| return r;

            const key = items[1].bulk_string;
            const startId = items[2].bulk_string;
            const endId = items[3].bulk_string;
            const range_slice = store.xrange(key, startId, endId) orelse {
                return .{ .response = .{ .array = try alloc.alloc(RespValue, 0) } };
            };
            const range_array = try alloc.alloc(RespValue, range_slice.len);
            for (range_slice, range_array) |record, *resp| {
                const id_str = try std.fmt.allocPrint(alloc, "{d}-{d}", .{ record.id.ms, record.id.sequence });
                const fields = try alloc.alloc(RespValue, record.fields.len);
                for (record.fields, fields) |f, *r| r.* = .{ .bulk_string = f };
                
                const entry_arr = try alloc.alloc(RespValue, 2);
                entry_arr[0] = .{ .bulk_string = id_str };
                entry_arr[1] = .{ .array = fields };
                resp.* = .{ .array = entry_arr };
            }
            return .{ .response = .{ .array = range_array } };
        },
    }
}

fn wrongArgs(items: []const RespValue, min: usize) ?Action {
    if (items.len < min) return .{ .response = .{ .error_msg = "wrong number of arguments" } };

    return null;
}

// Responsible for identifying the RESP command
// Checks if command is incomplete
// returns the ParseResult
pub fn parse(alloc: std.mem.Allocator, data: []const u8) !ParseResult {
    const first_newline = std.mem.indexOf(u8, data, "\r\n") orelse {
        return error.IncompleteCommand;
    };

    switch (data[0]) {
        '+' => return .{ // simple string
            .value = .{ .simple_string = data[1..first_newline] },
            .consumed = first_newline + 2,
        },
        '*' => { // array
            const count = std.fmt.parseInt(usize, data[1..first_newline], 10) catch {
                return error.ProtocolError;
            };
            var cursor = first_newline + 2;
            const items = try alloc.alloc(RespValue, count);
            for (0..count) |i| {
                const result = try parse(alloc, data[cursor..]);
                items[i] = result.value;
                cursor += result.consumed;
            }

            return .{
                .value = .{ .array = items },
                .consumed = cursor,
            };
        },
        '$' => { // bulk string
            const length = std.fmt.parseInt(usize, data[1..first_newline], 10) catch {
                return error.ProtocolError;
            };
            const cursor = first_newline + 2;
            return .{
                .value = .{ .bulk_string = data[cursor .. cursor + length] },
                .consumed = cursor + length + 2,
            };
        },
        else => return error.ProtocolError,
    }
}

pub fn serialize(alloc: std.mem.Allocator, resp_value: *const RespValue) ![]const u8 {
    switch (resp_value.*) {
        .null_value => return "$-1\r\n",
        .bulk_string => |s| {
            return try std.fmt.allocPrint(alloc, "${d}\r\n{s}\r\n", .{ s.len, s });
        },
        .simple_string => |s| {
            return try std.fmt.allocPrint(alloc, "+{s}\r\n", .{s});
        },
        .integer => |d| {
            return try std.fmt.allocPrint(alloc, ":{d}\r\n", .{d});
        },
        .array => |arr| {
            var a: std.io.Writer.Allocating = .init(alloc);
            const w = &a.writer;
            try w.print("*{d}\r\n", .{arr.len});
            for (arr) |*e| try w.print("{s}", .{try serialize(alloc, e)});
            return try a.toOwnedSlice();
        },
        .error_msg => |e| {
            return try std.fmt.allocPrint(alloc, "-ERR {s}\r\n", .{e});
        },
    }
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
        const action = try handleCommand(alloc, &self.store, .{ .array = resp_args });
        const resp = switch (action) {
            .response => |r| r,
            .push => |p| p.response,
            .block => return error.UnexpectedBlock,
        };
        return serialize(alloc, &resp);
    }

    fn cmdAction(self: *TestCtx, args: []const []const u8) !Action {
        const alloc = self.arena.allocator();
        const resp_args = try alloc.alloc(RespValue, args.len);
        for (args, resp_args) |arg, *resp| resp.* = .{ .bulk_string = arg };
        return handleCommand(alloc, &self.store, .{ .array = resp_args });
    }

    fn expect(self: *TestCtx, expected: []const u8, args: []const []const u8) !void {
        try std.testing.expectEqualStrings(expected, try self.cmd(args));
    }
};

test "handleCommand: PING returns PONG" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    try ctx.expect("+PONG\r\n", &.{"PING"});
}

test "handleCommand: ECHO replies" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    try ctx.expect("$5\r\nhello\r\n", &.{ "ECHO", "hello" });
}

test "handleCommand: SET returns OK" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    try ctx.expect("+OK\r\n", &.{ "SET", "foo", "bar" });
}

test "handleCommand: SET & GET returns key" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    _ = try ctx.cmd(&.{ "SET", "foo", "bar" });
    try ctx.expect("$3\r\nbar\r\n", &.{ "GET", "foo" });
}

test "handleCommand: GET returns RESP null bulk string on non-existant string" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    try ctx.expect("$-1\r\n", &.{ "GET", "foo" });
}

test "handleCommand: RPUSH creates a new list for new key" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    try ctx.expect(":1\r\n", &.{ "RPUSH", "foo", "bar" });
}

test "handleCommand: RPUSH appends to list for existing key" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    _ = try ctx.cmd(&.{ "RPUSH", "foo", "bar" });
    try ctx.expect(":2\r\n", &.{ "RPUSH", "foo", "bash" });
}

test "handleCommand: RPUSH handles multiple elements" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    try ctx.expect(":2\r\n", &.{ "RPUSH", "foo", "bar", "bash" });
    try ctx.expect(":3\r\n", &.{ "RPUSH", "foo", "titi" });
}

test "handleCommand: LPUSH creates a new list for new key" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    try ctx.expect(":1\r\n", &.{ "LPUSH", "foo", "bar" });
}

test "handleCommand: LPUSH appends to list for existing key" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    _ = try ctx.cmd(&.{ "LPUSH", "foo", "bar" });
    try ctx.expect(":2\r\n", &.{ "LPUSH", "foo", "bash" });
}

test "handleCommand: LPUSH handles multiple elements" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    try ctx.expect(":2\r\n", &.{ "LPUSH", "foo", "bar", "bash" });
    try ctx.expect(":3\r\n", &.{ "LPUSH", "foo", "titi" });
}

test "handleCommand: LRANGE returns elements" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    _ = try ctx.cmd(&.{ "RPUSH", "mylist", "one", "two", "three" });
    try ctx.expect("*3\r\n$3\r\none\r\n$3\r\ntwo\r\n$5\r\nthree\r\n", &.{ "LRANGE", "mylist", "0", "2" });
}

test "handleCommand: LRANGE stop beyond end clamps to list length" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    _ = try ctx.cmd(&.{ "RPUSH", "mylist", "a", "b" });
    try ctx.expect("*2\r\n$1\r\na\r\n$1\r\nb\r\n", &.{ "LRANGE", "mylist", "0", "100" });
}

test "handleCommand: LRANGE returns empty for non-existent key" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    try ctx.expect("*0\r\n", &.{ "LRANGE", "nokey", "0", "1" });
}

test "handleCommand: LRANGE negative stop returns full list" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    _ = try ctx.cmd(&.{ "RPUSH", "mylist", "one", "two", "three" });
    try ctx.expect("*3\r\n$3\r\none\r\n$3\r\ntwo\r\n$5\r\nthree\r\n", &.{ "LRANGE", "mylist", "0", "-1" });
}

test "handleCommand: LLEN returns length for existing list" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    _ = try ctx.cmd(&.{ "RPUSH", "list", "a", "b" });
    try ctx.expect(":2\r\n", &.{ "LLEN", "list" });
}

test "handleCommand: LLEN returns 0 for non-existent list" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    try ctx.expect(":0\r\n", &.{ "LLEN", "list" });
}

test "handleCommand: LPOP pops a single item from a list" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    _ = try ctx.cmd(&.{ "RPUSH", "list", "one", "two", "three", "four", "five" });
    try ctx.expect("$3\r\none\r\n", &.{ "LPOP", "list" });
}

test "handleCommand: LPOP pops multiple items from a list" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    _ = try ctx.cmd(&.{ "RPUSH", "list", "one", "two", "three", "four", "five" });
    try ctx.expect("*2\r\n$3\r\none\r\n$3\r\ntwo\r\n", &.{ "LPOP", "list", "2" });
}

test "handleCommand: LPOP returns null bulk string for empty list" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    _ = try ctx.cmd(&.{ "RPUSH", "list", "one" });
    _ = try ctx.cmd(&.{ "LPOP", "list" });
    try ctx.expect("$-1\r\n", &.{ "LPOP", "list" });
}

test "handleCommand: LPOP returns null bulk string for non-existent list" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    _ = try ctx.cmd(&.{ "LPOP", "list" });
    try ctx.expect("$-1\r\n", &.{ "LPOP", "list" });
}

test "handleCommand: BLPOP returns immediate response when list has data" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    _ = try ctx.cmd(&.{ "RPUSH", "mylist", "hello" });
    try ctx.expect("*2\r\n$6\r\nmylist\r\n$5\r\nhello\r\n", &.{ "BLPOP", "mylist", "0" });
}

test "handleCommand: BLPOP checks multiple keys in order" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    _ = try ctx.cmd(&.{ "RPUSH", "b", "val" });
    try ctx.expect("*2\r\n$1\r\nb\r\n$3\r\nval\r\n", &.{ "BLPOP", "a", "b", "0" });
}

test "handleCommand: BLPOP returns block action on empty list" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    const action = try ctx.cmdAction(&.{ "BLPOP", "mylist", "5" });
    switch (action) {
        .block => |b| {
            try std.testing.expectEqual(@as(usize, 1), b.keys.len);
            try std.testing.expectEqualStrings("mylist", b.keys[0]);
            try std.testing.expectEqual(@as(u64, 5000), b.timeout_ms);
        },
        else => return error.WrongAction,
    }
}

test "handleCommand: BLPOP timeout 0 means block forever" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    const action = try ctx.cmdAction(&.{ "BLPOP", "mylist", "0" });
    switch (action) {
        .block => |b| try std.testing.expectEqual(@as(u64, 0), b.timeout_ms),
        else => return error.WrongAction,
    }
}

test "handleCommand: RPUSH returns push action with correct key" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    const action = try ctx.cmdAction(&.{ "RPUSH", "mylist", "a" });
    switch (action) {
        .push => |p| {
            try std.testing.expectEqualStrings("mylist", p.key);
            const serialized = try serialize(ctx.arena.allocator(), &p.response);
            try std.testing.expectEqualStrings(":1\r\n", serialized);
        },
        else => return error.WrongAction,
    }
}

test "parse: returns error on incomplete data" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    try std.testing.expectError(error.IncompleteCommand, parse(arena.allocator(), "*1"));
    try std.testing.expectError(error.IncompleteCommand, parse(arena.allocator(), ""));
}

test "parse: partial array header only" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    try std.testing.expectError(error.IncompleteCommand, parse(arena.allocator(), "*2\r\n"));
}

test "parse: PING as RESP array" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const input = "*1\r\n$4\r\nPING\r\n";
    const result = try parse(arena.allocator(), input);
    try std.testing.expectEqual(input.len, result.consumed);
    switch (result.value) {
        .array => |items| {
            try std.testing.expectEqual(@as(usize, 1), items.len);
            try std.testing.expectEqualStrings("PING", items[0].bulk_string);
        },
        else => return error.WrongVariant,
    }
}

test "handleCommand: XADD creates stream and returns id" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    try ctx.expect("$3\r\n1-0\r\n", &.{ "XADD", "mystream", "1-0", "key", "value" });
}

test "handleCommand: XADD auto-sequences within same ms" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    _ = try ctx.cmd(&.{ "XADD", "mystream", "1-0", "a", "1" });
    try ctx.expect("$3\r\n1-1\r\n", &.{ "XADD", "mystream", "1-*", "b", "2" });
}

test "handleCommand: XADD rejects id equal or smaller than last" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    _ = try ctx.cmd(&.{ "XADD", "mystream", "2-0", "a", "1" });
    try ctx.expect("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n", &.{ "XADD", "mystream", "1-0", "b", "2" });
}

test "handleCommand: XADD rejects 0-0" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    try ctx.expect("-ERR The ID specified in XADD must be greater than 0-0\r\n", &.{ "XADD", "mystream", "0-0", "a", "1" });
}

test "handleCommand: XADD multiple entries" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    _ = try ctx.cmd(&.{ "XADD", "s", "1-0", "a", "1" });
    _ = try ctx.cmd(&.{ "XADD", "s", "2-0", "b", "2" });
    try ctx.expect("$3\r\n3-0\r\n", &.{ "XADD", "s", "3-0", "c", "3" });
}

test "handleCommand: XRANGE returns empty for non-existent key" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    try ctx.expect("*0\r\n", &.{ "XRANGE", "nostream", "0", "99" });
}

test "handleCommand: TYPE returns stream for stream key" {
    var ctx = TestCtx.init();
    defer ctx.deinit();
    _ = try ctx.cmd(&.{ "XADD", "mystream", "1-0", "k", "v" });
    try ctx.expect("+stream\r\n", &.{ "TYPE", "mystream" });
}

test "parse: ECHO as RESP array" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const input = "*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n";
    const result = try parse(arena.allocator(), input);
    try std.testing.expectEqual(input.len, result.consumed);
    switch (result.value) {
        .array => |items| {
            try std.testing.expectEqual(@as(usize, 2), items.len);
            try std.testing.expectEqualStrings("ECHO", items[0].bulk_string);
            try std.testing.expectEqualStrings("hello", items[1].bulk_string);
        },
        else => return error.WrongVariant,
    }
}
