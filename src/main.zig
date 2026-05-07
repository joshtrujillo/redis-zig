// src/main.zig

const std = @import("std");
const Server = @import("server.zig").Server;
const protocol = @import("protocol.zig");

pub fn main() !void {
    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();

    var server = try Server.init(alloc);
    defer server.deinit();

    while (true) {
        const poll_timeout_ms = server.computeTimeout();
        const events = try server.reactor.wait(poll_timeout_ms);
        server.expireBlockedClients();
        defer _ = arena.reset(.retain_capacity);

        for (events) |ev| {
            if (ev.fd == server.listener.stream.handle) {
                if (ev.readable) try server.acceptClient();
                continue;
            }

            if (ev.err) {
                server.removeClient(ev.fd);
                continue;
            }

            if (!ev.readable) continue;

            const client = server.clients.getPtr(ev.fd) orelse continue;

            const n = client.conn.recv() catch 0;
            if (n == 0) {
                std.log.info("Client disconnected - fd: {d}", .{ev.fd});
                server.removeClient(ev.fd);
                continue;
            }

            while (try client.parser.feed(arena.allocator(), client.conn.recv_buf.readableSlice())) |result| {
                std.log.info(
                    "Client fd: {d} sent command: {s}",
                    .{ ev.fd, result.value.array[0].bulk_string },
                );
                client.conn.recv_buf.advance(result.consumed);
                try server.dispatch(ev.fd, result.value, arena.allocator());
            }

            _ = try client.conn.flush();
        }
    }
}
