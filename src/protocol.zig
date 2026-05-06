// src/protocol.zig

const std = @import("std");

pub const RespValue = union(enum) {
    simple_string: []const u8,
    bulk_string: []const u8,
    integer: i64,
    array: []RespValue,
    null_value,
    error_msg: []const u8,

    pub fn bulkStringArray(alloc: std.mem.Allocator, strings: []const []const u8) ![]RespValue {
        const arr = try alloc.alloc(RespValue, strings.len);
        for (strings, arr) |s, *r| r.* = .{ .bulk_string = s };
        return arr;
    }
};

pub const Parser = struct {
    const max_depth = 8;

    state: State = .start,
    str_len: usize = 0,
    cursor: usize = 0,
    stack: [max_depth]Frame = undefined,
    depth: u4 = 0,

    pub const Result = struct {
        value: RespValue,
        consumed: usize,
    };

    const Frame = struct {
        items: []RespValue,
        filled: usize,
    };

    const State = enum { start, bulk_len, bulk_data, array_len, simple_string, integer, error_msg };

    pub fn feed(self: *Parser, arena: std.mem.Allocator, data: []const u8) !?Result {
        var cur = self.cursor;

        while (true) {
            switch (self.state) {
                // Read type byte
                .start => {
                    if (cur >= data.len) break;

                    self.state = switch (data[cur]) {
                        '*' => .array_len,
                        '$' => .bulk_len,
                        '+' => .simple_string,
                        ':' => .integer,
                        '-' => .error_msg,
                        else => return error.ProtocolError,
                    };
                    cur += 1;
                },

                // *N\r\n
                // Push a frame onto the stack
                .array_len => {
                    const nl = std.mem.indexOf(u8, data[cur..], "\r\n") orelse break;
                    const count = std.fmt.parseInt(usize, data[cur..][0..nl], 10) catch
                        return error.ProtocolError;
                    cur += nl + 2; // skip past new line

                    if (count == 0) {
                        const empty = try arena.alloc(RespValue, 0);
                        self.state = .start;
                        if (self.completeValue(.{ .array = empty })) |top| {
                            self.cursor = 0;
                            return .{ .value = top, .consumed = cur };
                        }
                    } else {
                        if (self.depth >= max_depth) return error.ProtocolError;
                        self.stack[self.depth] = .{
                            .items = try arena.alloc(RespValue, count),
                            .filled = 0,
                        };
                        self.depth += 1;
                        self.state = .start; // parse first element
                    }
                },

                // $N\r\n
                // Read the length, transition to bulk_data
                .bulk_len => {
                    const nl = std.mem.indexOf(u8, data[cur..], "\r\n") orelse break;
                    const len_str = data[cur..][0..nl];
                    cur += nl + 2;

                    // Check for RESP null
                    if (std.mem.eql(u8, len_str, "-1")) {
                        self.state = .start;
                        if (self.completeValue(.null_value)) |top| {
                            self.cursor = 0;
                            return .{ .value = top, .consumed = cur };
                        }
                    } else {
                        self.str_len = std.fmt.parseInt(usize, len_str, 10) catch
                            return error.ProtocolError;
                        self.state = .bulk_data;
                    }
                },

                // The N bytes + \r\n after $N\r\n
                .bulk_data => {
                    if (cur + self.str_len + 2 > data.len) break;
                    const value = RespValue{ .bulk_string = data[cur .. cur + self.str_len] };
                    cur += self.str_len + 2;
                    self.state = .start;
                    if (self.completeValue(value)) |top| {
                        self.cursor = 0;
                        return .{ .value = top, .consumed = cur };
                    }
                },

                // +text\r\n
                .simple_string => {
                    const nl = std.mem.indexOf(u8, data[cur..], "\r\n") orelse break;
                    const value = RespValue{ .simple_string = data[cur..][0..nl] };
                    cur += nl + 2;
                    self.state = .start;
                    if (self.completeValue(value)) |top| {
                        self.cursor = 0;
                        return .{ .value = top, .consumed = cur };
                    }
                },

                // :123\r\n
                .integer => {
                    const nl = std.mem.indexOf(u8, data[cur..], "\r\n") orelse break;
                    const n = std.fmt.parseInt(i64, data[cur..][0..nl], 10) catch
                        return error.ProtocolError;
                    cur += nl + 2;
                    self.state = .start;
                    if (self.completeValue(.{ .integer = n })) |top| {
                        self.cursor = 0;
                        return .{ .value = top, .consumed = cur };
                    }
                },

                // -ERR message\r\n
                .error_msg => {
                    const nl = std.mem.indexOf(u8, data[cur..], "\r\n") orelse break;
                    const value = RespValue{ .error_msg = data[cur..][0..nl] };
                    cur += nl + 2;
                    self.state = .start;
                    if (self.completeValue(value)) |top| {
                        self.cursor = 0;
                        return .{ .value = top, .consumed = cur };
                    }
                },
            }
        }
        self.cursor = cur;
        return null;
    }

    fn completeValue(self: *Parser, value: RespValue) ?RespValue {
        if (self.depth == 0) return value;

        var val = value;
        while (self.depth > 0) {
            const frame = &self.stack[self.depth - 1];
            frame.items[frame.filled] = val;
            frame.filled += 1;
            if (frame.filled < frame.items.len) return null; // frame not full yet
            // Pop full frame, the completed array becomes a value in the parent
            val = .{ .array = frame.items };
            self.depth -= 1;
        }
        return val; // popped all the way out
    }
};

pub fn serialize(w: *std.io.Writer, resp_value: *const RespValue) !void {
    switch (resp_value.*) {
        .null_value => try w.writeAll("$-1\r\n"),
        .bulk_string => |s| try w.print("${d}\r\n{s}\r\n", .{ s.len, s }),
        .simple_string => |s| try w.print("+{s}\r\n", .{s}),
        .integer => |d| try w.print(":{d}\r\n", .{d}),
        .array => |arr| {
            try w.print("*{d}\r\n", .{arr.len});
            for (arr) |*e| try serialize(w, e);
        },
        .error_msg => |e| try w.print("-ERR {s}\r\n", .{e}),
    }
}

// Tests

test "Parser.feed: returns null on incomplete data" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var p1: Parser = .{};
    try std.testing.expect(try p1.feed(arena.allocator(), "*1") == null);
    var p2: Parser = .{};
    try std.testing.expect(try p2.feed(arena.allocator(), "") == null);
}

test "Parser.feed: partial array header only" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var parser: Parser = .{};
    try std.testing.expect(try parser.feed(arena.allocator(), "*2\r\n") == null);
}

test "Parser.feed: PING as RESP array" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var parser: Parser = .{};
    const input = "*1\r\n$4\r\nPING\r\n";
    const result = try parser.feed(arena.allocator(), input) orelse return;
    try std.testing.expectEqual(input.len, result.consumed);
    switch (result.value) {
        .array => |items| {
            try std.testing.expectEqual(@as(usize, 1), items.len);
            try std.testing.expectEqualStrings("PING", items[0].bulk_string);
        },
        else => return error.WrongVariant,
    }
}

test "Parser.feed: ECHO as RESP array" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    var parser: Parser = .{};
    defer arena.deinit();
    const input = "*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n";
    const result = try parser.feed(arena.allocator(), input) orelse return;
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
