// src/protocol.zig

const std = @import("std");

pub fn handleCommand(input: []const u8) []const u8 {
    if (std.mem.eql(u8, input, "PING\r\n") or std.mem.eql(u8, input, "ping\r\n")) {
        return "+PONG\r\n";
    }
    return "-ERR unknown command\r\n";
}

test "ping pong logic" {
    const result = handleCommand("PING\r\n");
    try std.testing.expectEqualStrings("+PONG\r\n", result);
}
