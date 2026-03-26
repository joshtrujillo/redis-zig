const std = @import("std");
const stdout = std.fs.File.stdout();
const net = std.net;
const posix = std.posix;
const protocol = @import("protocol.zig");
const storage = @import("storage.zig");

const Client = struct {
    conn: net.Server.Connection,
    buf: [4096]u8 = undefined,
    buf_len: usize = 0,

    pub fn init(conn: net.Server.Connection) Client {
        return .{
            .conn = conn,
            .buf_len = 0,
        };
    }

    pub fn deinit(self: *Client) void {
        self.conn.stream.close();
    }
};

const BlockedClient = struct {
    keys: [][]const u8,
    deadline: ?i64,

    fn deinit(self: *BlockedClient, alloc: std.mem.Allocator) void {
        for (self.keys) |k| alloc.free(k);
        alloc.free(self.keys);
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
    var server = try address.listen(.{ .reuse_address = true });
    defer server.deinit();

    // Poll array setup
    const flags = try posix.fcntl(server.stream.handle, posix.F.GETFL, 0);
    _ = try posix.fcntl(server.stream.handle, posix.F.SETFL, flags | @as(u32, @bitCast(posix.O{ .NONBLOCK = true })));

    var poll_fds: std.ArrayList(posix.pollfd) = .empty;
    defer poll_fds.deinit(server_alloc);

    try poll_fds.append(server_alloc, .{
        .fd = server.stream.handle,
        .events = posix.POLL.IN,
        .revents = 0,
    });

    var clients = std.AutoHashMap(posix.socket_t, Client).init(server_alloc);
    defer {
        var it = clients.valueIterator();
        while (it.next()) |c| c.deinit();
        clients.deinit();
    }
    var blocked = std.AutoHashMap(posix.socket_t, BlockedClient).init(server_alloc);
    defer {
        var it = blocked.valueIterator();
        while (it.next()) |b| b.deinit(server_alloc);
        blocked.deinit();
    }

    // Arena allocator so we can just free everything at once
    var arena = std.heap.ArenaAllocator.init(server_alloc);
    defer arena.deinit();

    while (true) {
        var poll_timeout: i32 = -1;
        {
            const now = std.time.milliTimestamp();
            var it = blocked.iterator();
            while (it.next()) |e| {
                if (e.value_ptr.deadline) |dl| {
                    const ms: i32 = @intCast(@max(0, @min(dl - now, std.math.maxInt(i32))));
                    if (poll_timeout == -1 or ms < poll_timeout) poll_timeout = ms;
                }
            }

        }
        _ = try posix.poll(poll_fds.items, -1);
        {
            const now = std.time.milliTimestamp();
            var expired: std.ArrayList(posix.socket_t) = .empty;
            var it = blocked.iterator();
            while (it.next()) |e| {
                if (e.value_ptr.deadline) |dl|
                if (now >= dl) try expired.append(server_alloc, e.key_ptr.*);
            }
            for (expired.items) |fd| {
                var entry = blocked.fetchRemove(fd).?;
                entry.value.deinit(server_alloc);
                _ = posix.write(fd, "*-1\r\n") catch {};
            }
        }
        defer _ = arena.reset(.retain_capacity);
        const command_alloc = arena.allocator();

        // Check for new connections
        if (poll_fds.items[0].revents & posix.POLL.IN != 0) {
            const conn = try server.accept();

            try clients.put(conn.stream.handle, .{ .conn = conn });
            try poll_fds.append(server_alloc, .{
                .fd = conn.stream.handle,
                .events = posix.POLL.IN,
                .revents = 0,
            });
            std.log.info("Accepted connection: fd {d}", .{conn.stream.handle});
        }

        // Check Clients
        var i: usize = 1;
        while (i < poll_fds.items.len) {
            const pfd = &poll_fds.items[i];
            if (pfd.revents & posix.POLL.IN != 0) {
                const client = clients.getPtr(pfd.fd).?;

                // Read from client
                const available_space = client.buf[client.buf_len..];
                const n = posix.read(pfd.fd, available_space) catch 0;

                if (n == 0) {
                    // Client disconnected
                    std.log.info("Client disconnected: fd {d}", .{pfd.fd});
                    if (clients.fetchRemove(pfd.fd)) |entry| { var e = entry; e.value.deinit(); }
                    if (blocked.fetchRemove(pfd.fd)) |entry| { var e = entry; e.value.deinit(server_alloc); }
                    _ = poll_fds.swapRemove(i);
                    continue;
                }

                client.buf_len += n;

                const current_data = client.buf[0..client.buf_len];

                const result = protocol.parse(command_alloc, current_data) catch |err| {
                    if (err == error.IncompleteCommand) { i += 1; continue; }
                    return err;
                };

                var w = client.conn.stream.writer(&.{});
                switch (try protocol.handleCommand(command_alloc, &store, result.value)) {
                    .response => |r| try w.interface.writeAll(r),
                    .push => |p| {
                        try w.interface.writeAll(p.response);
                        try wakeBlocked(p.key, &store, &blocked, server_alloc, command_alloc);
                    },
                    .block => |b| {
                        const keys = try server_alloc.alloc([]const u8, b.keys.len);
                        for (b.keys, keys) |src, *dst| dst.* = try server_alloc.dupe(u8, src);
                        const deadline: ?i64 = if (b.timeout_ms == 0) null
                            else std.time.milliTimestamp() + @as(i64, @intCast(b.timeout_ms));
                        try blocked.put(pfd.fd, .{ .keys = keys, .deadline = deadline });
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
}

fn wakeBlocked(
    key: []const u8,
    store: *storage.Store,
    blocked: *std.AutoHashMap(posix.socket_t, BlockedClient),
    server_alloc: std.mem.Allocator,
    arena_alloc: std.mem.Allocator,
) !void {
    var it = blocked.iterator();
    while (it.next()) |e| {
        for (e.value_ptr.keys) |k| {
            if (!std.mem.eql(u8, k, key)) continue;

            const fd = e.key_ptr.*;
            var entry = blocked.fetchRemove(fd).?;
            defer entry.value.deinit(server_alloc);
            
            const popped = try store.lpop(arena_alloc, key, 1) orelse return;
            const response = try std.fmt.allocPrint(
                arena_alloc,
                "*2\r\n${d}\r\n{s}\r\n${d}\r\n{s}\r\n",
                .{ key.len, key, popped[0].len, popped[0] }
            );
            _ = posix.write(fd, response) catch {};
            return; // FIFO
        }
    }
}
