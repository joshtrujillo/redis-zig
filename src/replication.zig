// src/replication.zig

const std = @import("std");
const net = std.net;
const posix = std.posix;
const protocol = @import("protocol.zig");

pub fn connectToMaster(alloc: std.mem.Allocator, host: []const u8, port: u16) !net.Stream {
    // 1. TCP connect to master
    const stream = try net.tcpConnectToHost(alloc, host, port);

    var send_buf: [4096]u8 = undefined;
    var w: std.io.Writer = .fixed(&send_buf);

    // 2. Send PING, expect +PONG
    var ping_args = [_]protocol.RespValue{.{ .bulk_string = "PING" }};
    const ping = protocol.RespValue{ .array = &ping_args };

    try protocol.serialize(&w, &ping);
    const written = w.end;
    _ = try posix.write(stream.handle, send_buf[0..written]);

    // 3. Send REPLCONF listening-port <port>, expect +OK
    // 4. Send REPLCONF capa psync2, expect +OK
    // 5. Send PSYNC ? -1, expect +FULLSYNC <replid> <offset>
    // return the connected stream
    return stream;
}
