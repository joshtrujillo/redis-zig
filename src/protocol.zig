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


pub fn handleCommand(value: RespValue) []const u8 {
    switch (value) {
        // .simple_string => |str| {},
        // .bulk_string   => {}, // |str| {},
        .array         => |items| {
            const cmd = items[0].bulk_string;
            if (std.ascii.eqlIgnoreCase(u8, cmd, "PING")) return "+PONG\r\n";
        },
        // .integer       => |n| {},
        // .null_value    => {},
        // .error_msg     => |e| {},
        else           => {},
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
        '+' => return .{
            .value = .{ .simple_string = data[1..first_newline] },
            .consumed = first_newline + 2,
        },
        '*' => {
            const count = std.fmt.parseInt(usize, data[1..first_newline], 10) catch {
                return error.ProtocolError;
            };
            var cursor = first_newline + 2;
            const items = try alloc.alloc(RespValue, count);
            errdefer alloc.free(items);
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
        '$' => {
            // bulk string
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
    var args = [_]RespValue{.{ .bulk_string = "PING" }};
    try std.testing.expectEqualStrings("+PONG\r\n", handleCommand(.{ .array = &args }));
}

test "parse: returns error on incomplete data" {
    try std.testing.expectError(error.IncompleteCommand, parse(std.testing.allocator, "*1"));
    try std.testing.expectError(error.IncompleteCommand, parse(std.testing.allocator, ""));
}

test "parse: partial array header only" {
    try std.testing.expectError(error.IncompleteCommand, parse(std.testing.allocator, "*2\r\n"));
}

test "parse: PING as RESP array" {
    const input = "*1\r\n$4\r\nPING\r\n";
    const result = try parse(std.testing.allocator, input);
    defer std.testing.allocator.free(result.value.array);
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
    const input = "*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n";
    const result = try parse(std.testing.allocator, input);
    defer std.testing.allocator.free(result.value.array);
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
