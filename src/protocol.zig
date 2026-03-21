// src/protocol.zig

const std = @import("std");
const posix = std.posix;

pub fn handleCommand(input: []const u8) []const u8 {
    if (std.mem.eql(u8, input, "PING\r\n") or std.mem.eql(u8, input, "ping\r\n")) {
        return "+PONG\r\n";
    }
    return "-ERR unknown command\r\n";
}

// Responsible for identifying the RESP command
// Checks if command is incomplete
// returns the number of bytes processed
pub fn parseAndHandle(fd: posix.socket_t, data: []const u8) !usize {
    // Find first new line
    const first_newline = std.mem.indexOf(u8, data, "\r\n") orelse {
        return error.IncompleteCommand;
    };
    
    // Determine the type from first byte
    const prefix = data[0];

    switch (prefix) {
        // Simple String
        '+' => {
            const end = first_newline + 2;
            const msg = data[1..first_newline];
            try processSimpleString(fd, msg);
            return end;
        },
        // Array
        '*' => {
            // We have to check if all elements have arrived
            return try parseArray(fd, data);
        },
        else => return error.ProtocolError,
    }
}

fn processSimpleString(_: posix.socket_t, _: []const u8) !void {}

fn parseArray(_: posix.socket_t, _: []const u8) !usize {}

test "ping pong logic" {
    const result = handleCommand("PING\r\n");
    try std.testing.expectEqualStrings("+PONG\r\n", result);
}
