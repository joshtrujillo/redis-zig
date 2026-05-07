// src/main.zig

const std = @import("std");
const net = std.net;
const posix = std.posix;
const el = @import("event_loop.zig");
const netx = @import("network.zig");
const protocol = @import("protocol.zig");
const storage = @import("storage.zig");
const engine = @import("engine.zig");

pub const Client = struct {
    conn: netx.Connection,
    parser: protocol.Parser = .{},
    queued_commands: ?std.ArrayList(protocol.RespValue) = null,

    pub fn deinit(self: *Client, alloc: std.mem.Allocator) void {
        if (self.queued_commands) |*q| q.deinit(alloc);
        self.conn.deinit(alloc);
        self.* = undefined;
    }
};

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

    var clients = std.AutoHashMap(posix.socket_t, Client).init(server_alloc);

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
            if (clients.getPtr(fd)) |client| {
                client.conn.queueSend(server_alloc, "*-1\r\n") catch {};
                _ = client.conn.flush() catch {};
            }
        }

        defer _ = arena.reset(.retain_capacity);

        for (events) |ev| {
            if (ev.fd == srv.stream.handle) {
                if (ev.readable) {
                    const conn = try srv.accept();
                    const client = Client{
                        .conn = try netx.Connection.init(conn.stream),
                    };
                    try clients.put(conn.stream.handle, client);
                    try reactor.register(conn.stream.handle);
                    std.log.info("Accepted connection - fd: {d}", .{conn.stream.handle});
                }
                continue;
            }

            if (ev.err) {
                reactor.unregister(ev.fd);
                if (clients.fetchRemove(ev.fd)) |entry| {
                    var c = entry.value;
                    c.deinit(server_alloc);
                }
                _ = blocked.fetchRemove(ev.fd);
                continue;
            }

            if (!ev.readable) continue;

            const client = clients.getPtr(ev.fd) orelse continue;

            const n = client.conn.recv() catch 0;
            if (n == 0) {
                std.log.info("Client disconnected - fd: {d}", .{ev.fd});
                reactor.unregister(ev.fd);
                _ = blocked.fetchRemove(ev.fd);
                if (clients.fetchRemove(ev.fd)) |entry| {
                    var c = entry.value;
                    c.deinit(server_alloc);
                }
                continue;
            }

            // Parse and execute all complete commands in the buffer
            while (try client.parser.feed(arena.allocator(), client.conn.recv_buf.readableSlice())) |result| {
                std.log.info(
                    "Client fd: {d} sent command: {s}",
                    .{ ev.fd, result.value.array[0].bulk_string },
                );

                client.conn.recv_buf.advance(result.consumed);
                try processCommand(client, result.value, ev.fd, &store, &blocked, &clients, server_alloc, arena.allocator());
            }

            _ = try client.conn.flush();
        }
    }
}

fn sendReply(client: *Client, alloc: std.mem.Allocator, reply: *const protocol.RespValue) !void {
    var w: std.io.Writer.Allocating = .fromArrayList(alloc, &client.conn.send_buf);
    try protocol.serialize(&w.writer, reply);
    client.conn.send_buf = w.toArrayList();
}

fn processCommand(
    client: *Client,
    value: protocol.RespValue,
    fd: posix.socket_t,
    store: *storage.Store,
    blocked: *std.AutoHashMap(posix.socket_t, engine.BlockedClient),
    clients: *std.AutoHashMap(posix.socket_t, Client),
    server_alloc: std.mem.Allocator,
    arena: std.mem.Allocator,
) !void {
    const cmd_name = value.array[0].bulk_string;

    // Handle MULTI, EXEC, and DISCARD
    if (client.queued_commands) |*queue| {
        // in MULTI mode
        if (std.ascii.eqlIgnoreCase(cmd_name, "EXEC")) {
            // drain queue, execute each, collect replies
            const reply = execQueue(queue);
            client.queued_commands = null;
            return sendReply(client, server_alloc, &reply);
        }
        if (std.ascii.eqlIgnoreCase(cmd_name, "DISCARD")) {
            // clear queue, set to null, reply +OK
            return;
        }
        if (std.ascii.eqlIgnoreCase(cmd_name, "MULTI")) {
            // reply -ERR MULTI calls can not be nested
            return sendReply(client, server_alloc, &.{ .error_msg = "MULTI calls can not be nested" });
        }
        // queue the command, reply +QUEUED
        try queue.append(server_alloc, value);
        return sendReply(client, server_alloc, &.{ .simple_string = "QUEUED"});
    }

    // Enter MULTI
    if (std.ascii.eqlIgnoreCase(cmd_name, "MULTI")) {
        client.queued_commands = .empty;
        return sendReply(client, server_alloc, &.{ .simple_string = "OK" });
    }

    if (std.ascii.eqlIgnoreCase(cmd_name, "EXEC")) {
        return sendReply(client, server_alloc, &.{ .error_msg = "EXEC without MULTI"});
    }

    // Normal execution
    switch (try engine.execute(arena, store, value)) {
        .reply => |r| try sendReply(client, server_alloc, &r),
        .reply_and_wake => |r| {
            try sendReply(client, server_alloc, &r.reply);
            std.log.info("wake: key={s} blocked_count={d}", .{ r.wake_key, blocked.count() });
            if (try engine.resolveWake(r.wake_key, store, blocked, server_alloc, arena)) |wake| {
                if (clients.getPtr(wake.fd)) |wake_client| {
                    try sendReply(wake_client, server_alloc, &wake.response);
                    _ = try wake_client.conn.flush();
                }
            }
        },
        .block => |b| try engine.blockClient(blocked, server_alloc, fd, b),
    }
}

fn execQueue(_: *std.ArrayList(protocol.RespValue)) protocol.RespValue {
    return .{ .array = &.{} };
}
