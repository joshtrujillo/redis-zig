const std = @import("std");
const net = std.net;
const posix = std.posix;
const protocol = @import("protocol.zig");
const storage = @import("storage.zig");
const server = @import("server.zig");

const Client = struct {
    conn: net.Server.Connection,
    buf: [4096]u8 = undefined,
    buf_len: usize = 0,

    pub fn deinit(self: *Client) void {
        self.conn.stream.close();
    }
};

pub fn main() !void {
    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const server_alloc = gpa.allocator();

    var store = storage.Store.init(server_alloc);
    defer store.deinit();

    // Server setup
    const address = try net.Address.resolveIp("127.0.0.1", 6379);
    var srv = try address.listen(.{ .reuse_address = true });
    defer srv.deinit();

    // Poll array setup
    const flags = try posix.fcntl(srv.stream.handle, posix.F.GETFL, 0);
    _ = try posix.fcntl(srv.stream.handle, posix.F.SETFL, flags | @as(u32, @bitCast(posix.O{ .NONBLOCK = true })));

    var poll_fds: std.ArrayList(posix.pollfd) = .empty;
    defer poll_fds.deinit(server_alloc);

    try poll_fds.append(server_alloc, .{
        .fd = srv.stream.handle,
        .events = posix.POLL.IN,
        .revents = 0,
    });

    var clients = std.AutoHashMap(posix.socket_t, Client).init(server_alloc);
    defer {
        var it = clients.valueIterator();
        while (it.next()) |c| c.deinit();
        clients.deinit();
    }
    var blocked = std.AutoHashMap(posix.socket_t, server.BlockedClient).init(server_alloc);
    defer {
        var it = blocked.valueIterator();
        while (it.next()) |b| b.deinit(server_alloc);
        blocked.deinit();
    }

    // Arena allocator so we can just free everything at once
    var arena = std.heap.ArenaAllocator.init(server_alloc);
    defer arena.deinit();

    // Server runtime loop
    while (true) {
        const poll_timeout_ms = server.computePollTimeout(&blocked);
        _ = try posix.poll(poll_fds.items, poll_timeout_ms);

        // Expire blocked clients and send null responses
        const expired = try server.expireBlocked(server_alloc, &blocked);
        defer server_alloc.free(expired);
        for (expired) |fd| {
            const stream = net.Stream{ .handle = fd };
            var ew = stream.writer(&.{});
            ew.interface.writeAll("*-1\r\n") catch {};
        }

        defer _ = arena.reset(.retain_capacity);

        // Check for new connections
        if (poll_fds.items[0].revents & posix.POLL.IN != 0) {
            try acceptConnection(server_alloc, &srv, &clients, &poll_fds);
        }

        try handleClientData(server_alloc, &arena, &store, &clients, &blocked, &poll_fds);
    }
}

fn acceptConnection(server_alloc: std.mem.Allocator, srv: *net.Server, clients: *std.AutoHashMap(posix.socket_t, Client), poll_fds: *std.ArrayList(posix.pollfd)) !void {
    const conn = try srv.accept();

    try clients.put(conn.stream.handle, .{ .conn = conn });
    try poll_fds.append(server_alloc, .{
        .fd = conn.stream.handle,
        .events = posix.POLL.IN,
        .revents = 0,
    });
    std.log.info("Accepted connection - fd: {d}", .{conn.stream.handle});
}

fn handleClientData(server_alloc: std.mem.Allocator, arena: *std.heap.ArenaAllocator, store: *storage.Store, clients: *std.AutoHashMap(posix.socket_t, Client), blocked: *std.AutoHashMap(posix.socket_t, server.BlockedClient), poll_fds: *std.ArrayList(posix.pollfd)) !void {
    var i: usize = 1;
    while (i < poll_fds.items.len) {
        const pfd = poll_fds.items[i];
        if (pfd.revents & posix.POLL.IN != 0) {
            const client = clients.getPtr(pfd.fd).?;

            // Read from client
            const available_space = client.buf[client.buf_len..];
            const n = posix.read(pfd.fd, available_space) catch 0;

            if (n == 0) {
                std.log.info("Client disconnected - fd: {d}", .{pfd.fd});
                if (clients.fetchRemove(pfd.fd)) |entry| {
                    var e = entry;
                    e.value.deinit();
                }
                if (blocked.fetchRemove(pfd.fd)) |entry| {
                    var e = entry;
                    e.value.deinit(server_alloc);
                }
                _ = poll_fds.swapRemove(i);
                continue;
            }

            client.buf_len += n;

            const current_data = client.buf[0..client.buf_len];
            const command_arena = arena.allocator();

            const result = protocol.parse(command_arena, current_data) catch |err| switch (err) {
                error.IncompleteCommand => {
                    i += 1;
                    continue;
                },
                else => |e| return e,
            };
            std.log.info("Client fd: {d} sent command: {s}", .{ pfd.fd, result.value.array[0].bulk_string });

            var w = client.conn.stream.writer(&.{});
            switch (try protocol.handleCommand(command_arena, store, result.value)) {
                .response => |r| {
                    try protocol.serialize(&w.interface, &r);
                },
                .wake => |r| {
                    try protocol.serialize(&w.interface, &r.response);
                    if (try server.resolveWake(r.key, store, blocked, server_alloc, command_arena)) |wake| {
                        const s = net.Stream{ .handle = wake.fd };
                        var ww = s.writer(&.{});
                        try protocol.serialize(&ww.interface, &wake.response);
                    }
                },
                .block => |b| {
                    const keys = try server_alloc.alloc([]const u8, b.keys.len);
                    const operation: protocol.BlockedOp = switch (b.operation) {
                        .blpop => .{ .blpop = {} },
                        .xread => |x| blk: {
                            const ids = try server_alloc.alloc([]const u8, x.ids.len);
                            for (x.ids, ids) |src, *dst| dst.* = try server_alloc.dupe(u8, src);
                            break :blk .{ .xread = .{ .ids = ids } };
                        },
                    };

                    for (b.keys, keys) |src, *dst| dst.* = try server_alloc.dupe(u8, src);
                    const deadline_ms: ?i64 = if (b.timeout_ms == 0) null else std.time.milliTimestamp() + @as(i64, @intCast(b.timeout_ms));
                    try blocked.put(pfd.fd, .{ .keys = keys, .deadline_ms = deadline_ms, .operation = operation });
                },
            }

            // Shift remaining data to the front of the buffer
            const remaining = client.buf_len - result.consumed;
            std.mem.copyForwards(u8, client.buf[0..remaining], client.buf[result.consumed..client.buf_len]);
            client.buf_len = remaining;
        }
        i += 1;
    }
}
