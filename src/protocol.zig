// src/protocol.zig

const std = @import("std");
const storage = @import("storage.zig");

const RespValue = union(enum) {
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

const Command = enum { PING, ECHO, SET, GET };

const NULL_STRING = "$-1\r\n";

// Handles the action taken for each RESP command
// returns the direct RESP reply to be written to the client's socket
pub fn handleCommand(alloc: std.mem.Allocator, store: *storage.Store, value: RespValue) ![]const u8 {
    const items = switch (value) {
        .array => |arr| arr,
        else => return "-ERR empty array\r\n",
    };

    const cmd = items[0].bulk_string;
    const upper = try std.ascii.allocUpperString(alloc, cmd);
    const command = std.meta.stringToEnum(Command, upper) orelse return "-ERR unknown command\r\n";

    switch (command) {
        .PING => return "+PONG\r\n",
        .ECHO => {
            if (items.len < 2) return "-ERR wrong number of arguments\r\n";
            const arg = items[1].bulk_string;
            return try std.fmt.allocPrint(alloc, "${d}\r\n{s}\r\n", .{ arg.len, arg });
        },
        .SET => {
            if (items.len < 3) return "-ERR wrong number of arguments\r\n";
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
            return "+OK\r\n";
        },
        .GET => {
            if (items.len < 2) return "-ERR wrong number of arguments\r\n";
            const key = items[1].bulk_string;
            if (store.get(key)) |v| {
                return try std.fmt.allocPrint(alloc, "${d}\r\n{s}\r\n", .{ v.len, v})   ;
            } else {
                return NULL_STRING;
            }
        }
    }
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
                .value = .{ .bulk_string = data[cursor..cursor+length] },
                .consumed = cursor + length + 2,
            };
        },
        else => return error.ProtocolError,
    }
}

fn testStore() storage.Store {
    return storage.Store.init(std.testing.allocator);
}

test "handleCommand: PING returns PONG" {
    var store = testStore();
    defer store.deinit();

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var args = [_]RespValue{.{ .bulk_string = "PING" }};
    const result = try handleCommand(arena.allocator(), &store, .{ .array = &args });
    try std.testing.expectEqualStrings("+PONG\r\n", result);
}

test "handleCommand: ECHO replies" {
    var store = testStore();
    defer store.deinit();

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var args = [_]RespValue{
        .{ .bulk_string = "ECHO" },
        .{ .bulk_string = "hello" },
    };
    const result = try handleCommand(arena.allocator(), &store, .{ .array = &args });
    try std.testing.expectEqualStrings("$5\r\nhello\r\n", result);
}

test "handleCommand: SET returns OK" {
    var store = testStore();
    defer store.deinit();

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var args = [_]RespValue{
        .{ .bulk_string = "SET" },
        .{ .bulk_string = "foo" },
        .{ .bulk_string = "bar" },
    };
    const result = try handleCommand(arena.allocator(), &store, .{ .array = &args });
    try std.testing.expectEqualStrings("+OK\r\n", result);
}

test "handleCommand: SET & GET returns key" {
    var store = testStore();
    defer store.deinit();

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var set_args = [_]RespValue{
        .{ .bulk_string = "SET" },
        .{ .bulk_string = "foo" },
        .{ .bulk_string = "bar" },
    };

    _ = try handleCommand(arena.allocator(), &store, .{ .array = &set_args });
    
    var get_args = [_]RespValue{
        .{ .bulk_string = "GET" },
        .{ .bulk_string = "foo" },
    };

    const result = try handleCommand(arena.allocator(), &store, .{ .array = &get_args });
    try std.testing.expectEqualStrings("$3\r\nbar\r\n", result);
}

test "handleCommand: GET returns RESP null bulk string on non-existant string" {
    var store = testStore();
    defer store.deinit();

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var get_args = [_]RespValue{
        .{ .bulk_string = "GET" },
        .{ .bulk_string = "foo" },
    };

    const result = try handleCommand(arena.allocator(), &store, .{ .array = &get_args });
    try std.testing.expectEqualStrings("$-1\r\n", result);
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
