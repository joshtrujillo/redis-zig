// src/replication.zig

const std = @import("std");
const net = std.net;
const protocol = @import("protocol.zig");

pub fn connectToMaster(host: []const u8, port: u16) !net.Stream {
    // 1. TCP connect to master
    const address = try net.Address.resolveIp(host, port);
    const listener = try address.listen(.{ .reuse_address = true });
    const conn = try listener.accept();
    const buf: [4096]u8 = undefined;
    const w: std.io.Writer = .fixed(buf);
    try protocol.serialize(w, &protocol.RespValue{ .array = protocol.RespValue{ .bulk_string = "PING" } });
    // 2. Send PING, expect +PONG

    // 3. Send REPLCONF listening-port <port>, expect +OK
    // 4. Send REPLCONF capa psync2, expect +OK
    // 5. Send PSYNC ? -1, expect +FULLSYNC <replid> <offset>
    // return the connected stream
    return conn.stream;
}
