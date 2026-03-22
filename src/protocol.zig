// src/protocol.zig

const std = @import("std");

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

const Command = enum { PING, ECHO };

pub fn handleCommand(alloc: std.mem.Allocator, value: RespValue) ![]const u8 {
    switch (value) {
        .array         => |items| {
            if (items.len == 0) return "-ERR empty array\r\n";

            const cmd = items[0].bulk_string;
            const upper = try std.ascii.allocUpperString(alloc, cmd);
            const command = std.meta.stringToEnum(Command, upper) orelse return "-ERR unknown command\r\n";

            switch (command) {
                .PING => return try std.fmt.allocPrint(alloc, "+PONG\r\n", .{}),
                .ECHO => {
                    const arg = items[1].bulk_string;
                    return try std.fmt.allocPrint(alloc, "${d}\r\n{s}\r\n", .{ arg.len, arg });
                }

            }
            
            if (std.ascii.eqlIgnoreCase(cmd, "PING")) {
            }
            if (std.ascii.eqlIgnoreCase(cmd, "ECHO")) {
                if (items.len < 2) return "-ERR wrong number of arguments\r\n";
            }
        },
        else => {},
    }
    return "-ERR unknown command\r\n";
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

test "handleCommand: PING returns PONG" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var args = [_]RespValue{.{ .bulk_string = "PING" }};
    const result = try handleCommand(arena.allocator(), .{ .array = &args });
    try std.testing.expectEqualStrings("+PONG\r\n", result);
}

test "handleCommand: ECHO replies" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var args = [_]RespValue{
        .{ .bulk_string = "ECHO" },
        .{ .bulk_string = "hello" },
    };
    const result = try handleCommand(arena.allocator(), .{ .array = &args });
    try std.testing.expectEqualStrings("$5\r\nhello\r\n", result);
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
