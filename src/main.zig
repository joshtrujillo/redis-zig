// src/main.zig

const std = @import("std");
const net = std.net;
const posix = std.posix;
const el = @import("event_loop.zig");
const netx = @import("network.zig");
const protocol = @import("protocol.zig");
const storage = @import("storage.zig");
const engine = @import("engine.zig");

pub fn main() !void {
    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const server_alloc = gpa.allocator();
    // Arena allocator so we can just free everything at once
    var arena = std.heap.ArenaAllocator.init(server_alloc);
    defer arena.deinit();

    var store = storage.Store.init(server_alloc);
    defer store.deinit();

    // Server setup
    const address = try net.Address.resolveIp("127.0.0.1", 6379);
    var srv = try address.listen(.{ .reuse_address = true });
    defer srv.deinit();

    var reactor = try el.Reactor(el.PollBackend).init(server_alloc);
    try reactor.register(srv.stream.handle);

    var connections = std.AutoHashMap(posix.socket_t, netx.Connection).init(server_alloc);

    var blocked = std.AutoHashMap(posix.socket_t, engine.BlockedClient).init(server_alloc);
    defer {
        var it = blocked.valueIterator();
        while (it.next()) |b| b.deinit(server_alloc);
        blocked.deinit();
    }

    // Server runtime loop
    while (true) {
        const poll_timeout_ms = engine.computeTimeout(&blocked);
        const events = try reactor.wait(poll_timeout_ms);

        // Expire blocked clients and send null responses
        const expired = try engine.expireBlocked(server_alloc, &blocked);
        defer server_alloc.free(expired);
        for (expired) |fd| {
            if (connections.getPtr(fd)) |conn| {
                conn.queueSend(server_alloc, "*-1\r\n") catch {};
                _ = conn.flush() catch {};
            }
        }

        defer _ = arena.reset(.retain_capacity);

        for (events) |ev| {
            if (ev.fd == srv.stream.handle) {
                if (ev.readable) {
                    const conn = try srv.accept();
                    try connections.put(conn.stream.handle, try netx.Connection.init(conn.stream));
                    try reactor.register(conn.stream.handle);
                    std.log.info("Accepted connection - fd: {d}", .{conn.stream.handle});
                }
                continue;
            }

            if (ev.err) {
                reactor.unregister(ev.fd);
                if (connections.fetchRemove(ev.fd)) |entry| {
                    var c = entry.value;
                    c.deinit(server_alloc);
                }
                _ = blocked.fetchRemove(ev.fd);
                continue;
            }

            if (!ev.readable) continue;

            const connection = connections.getPtr(ev.fd) orelse continue;

            const n = connection.recv() catch 0;
            if (n == 0) {
                std.log.info("Client disconnected - fd: {d}", .{ev.fd});
                reactor.unregister(ev.fd);
                _ = blocked.fetchRemove(ev.fd);
                if (connections.fetchRemove(ev.fd)) |entry| {
                    var c = entry.value;
                    c.deinit(server_alloc);
                }
                continue;
            }

            // Parse and execute all complete commands in the buffer
            while (try connection.parser.feed(arena.allocator(), connection.recv_buf.readableSlice())) |result| {
                std.log.info(
                    "Client fd: {d} sent command: {s}",
                    .{ ev.fd, result.value.array[0].bulk_string },
                );

                connection.recv_buf.advance(result.consumed);

                switch (try engine.execute(arena.allocator(), &store, result.value)) {
                    .reply => |r| {
                        var w: std.io.Writer.Allocating = .fromArrayList(server_alloc, &connection.send_buf);
                        try protocol.serialize(&w.writer, &r);
                        connection.send_buf = w.toArrayList();
                    },
                    .reply_and_wake => |r| {
                        var w: std.io.Writer.Allocating = .fromArrayList(server_alloc, &connection.send_buf);
                        try protocol.serialize(&w.writer, &r.reply);
                        connection.send_buf = w.toArrayList();
                        std.log.info("wake: key={s} blocked_count={d}", .{ r.wake_key, blocked.count() });
                        if (try engine.resolveWake(r.wake_key, &store, &blocked, server_alloc, arena.allocator())) |wake| {
                            if (connections.getPtr(wake.fd)) |wake_conn| {
                                var ww: std.io.Writer.Allocating = .fromArrayList(server_alloc, &wake_conn.send_buf);
                                try protocol.serialize(&ww.writer, &wake.response);
                                wake_conn.send_buf = ww.toArrayList();
                                _ = try wake_conn.flush();
                            }
                        }
                    },
                    .block => |b| {
                        try engine.blockClient(&blocked, server_alloc, ev.fd, b);
                    },
                }
            }

            _ = try connection.flush();
        }
    }
}
