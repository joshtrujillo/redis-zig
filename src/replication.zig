// src/replication.zig

const std = @import("std");
const net = std.net;
const posix = std.posix;
const protocol = @import("protocol.zig");

pub fn connectToMaster(alloc: std.mem.Allocator, host: []const u8, port: u16, listening_port: u16) !net.Stream {
    // 1. TCP connect to master
    const stream = try net.tcpConnectToHost(alloc, host, port);

    var send_buf: [4096]u8 = undefined;
    var recv_buf: [4096]u8 = undefined;
    var w: std.io.Writer = .fixed(&send_buf);

    // 2. Send PING, expect +PONG
    var ping_args = [_]protocol.RespValue{.{ .bulk_string = "PING" }};
    _ = try sendCommand(stream, &w, &send_buf, &recv_buf, &ping_args);

    // 3. Send REPLCONF listening-port <port>, expect +OK
    var port_buf: [5]u8 = undefined;
    const port_str = std.fmt.bufPrint(&port_buf, "{d}", .{listening_port}) catch unreachable;
    var replconf1_args = [_]protocol.RespValue{ .{ .bulk_string = "REPLCONF" }, .{ .bulk_string = "listening-port" }, .{ .bulk_string = port_str } };
    _ = try sendCommand(stream, &w, &send_buf, &recv_buf, &replconf1_args);

    // 4. Send REPLCONF capa psync2, expect +OK
    var replconf2_args = [_]protocol.RespValue{ .{ .bulk_string = "REPLCONF" }, .{ .bulk_string = "capa" }, .{ .bulk_string = "psync2" } };
    _ = try sendCommand(stream, &w, &send_buf, &recv_buf, &replconf2_args);

    // 5. Send PSYNC ? -1, expect +FULLRESYNC <replid> <offset>
    var psync_args = [_]protocol.RespValue{ .{ .bulk_string = "PSYNC" }, .{ .bulk_string = "?" }, .{ .bulk_string = "-1" } };
    _ = try sendCommand(stream, &w, &send_buf, &recv_buf, &psync_args);

    return stream;
}

fn sendCommand(stream: net.Stream, w: *std.io.Writer, send_buf: []u8, recv_buf: []u8, args: []protocol.RespValue) ![]const u8 {
    w.end = 0;
    const cmd = protocol.RespValue{ .array = args };
    try protocol.serialize(w, &cmd);
    _ = try posix.write(stream.handle, send_buf[0..w.end]);

    const n = try posix.read(stream.handle, recv_buf);
    return recv_buf[0..n];
}
