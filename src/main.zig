const std = @import("std");
const net = std.net;
const posix = std.posix;
const protocol = @import("protocol.zig");
const storage = @import("storage.zig");

const Client = struct {
    conn: net.Server.Connection,
    buf: [4096]u8 = undefined,
    buf_len: usize = 0,

    pub fn deinit(self: *Client) void {
        self.conn.stream.close();
    }
};

const BlockedClient = struct {
    deadline_ms: ?i64,
    keys: [][]const u8,
    operation: protocol.BlockedOp = .{ .blpop = {} },

    fn deinit(self: *BlockedClient, alloc: std.mem.Allocator) void {
        for (self.keys) |k| alloc.free(k);
        alloc.free(self.keys);
        switch (self.operation) {
            .xread => |x| {
                for (x.ids) |id| alloc.free(id);
                alloc.free(x.ids);
            },
            .blpop => {},
        }
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

    // Server runtime loop
    while (true) {
        const poll_timeout_ms = computePollTimeout(&blocked);
        _ = try posix.poll(poll_fds.items, poll_timeout_ms);

        try expireBlockedClients(server_alloc, &blocked);

        defer _ = arena.reset(.retain_capacity);

        // Check for new connections
        if (poll_fds.items[0].revents & posix.POLL.IN != 0) {
            try acceptConnection(server_alloc, &server, &clients, &poll_fds);
        }

        try handleClientData(server_alloc, &arena, &store, &clients, &blocked, &poll_fds);
    }
}

// Compute the shortest poll timeout across all blocked clients so
// that we wake up in time for the earliest deadline.
// -1 means "wait forever" (no blocked clients with deadlines).
fn computePollTimeout(blocked: *std.AutoHashMap(posix.socket_t, BlockedClient)) i32 {
    var poll_timeout_ms: i32 = -1;
    const now = std.time.milliTimestamp();
    var it = blocked.iterator();
    while (it.next()) |e| {
        if (e.value_ptr.deadline_ms) |dl| {
            // Clamp to [0, maxInt(i32)] to avoid negative/overflow values
            const ms: i32 = @intCast(@max(0, @min(dl - now, std.math.maxInt(i32))));
            if (poll_timeout_ms == -1 or ms < poll_timeout_ms) poll_timeout_ms = ms;
        }
    }

    return poll_timeout_ms;
}

// After poll returns, check which blocked clients have expired.
// Collect expired fds first, then remove them — can't modify the
// hashmap while iterating it.
fn expireBlockedClients(server_alloc: std.mem.Allocator, blocked: *std.AutoHashMap(posix.socket_t, BlockedClient),) !void {
    const now_ms = std.time.milliTimestamp();
    var expired: std.ArrayList(posix.socket_t) = .empty;
    defer expired.deinit(server_alloc);
    var it = blocked.iterator();
    while (it.next()) |e| {
        if (e.value_ptr.deadline_ms) |dl_ms|
            if (now_ms >= dl_ms) try expired.append(server_alloc, e.key_ptr.*);
    }
    // Send a null array response to each timed-out client
    for (expired.items) |fd| {
        var entry = blocked.fetchRemove(fd).?;
        entry.value.deinit(server_alloc);
        const stream = net.Stream{ .handle = fd };
        var ew = stream.writer(&.{});
        ew.interface.writeAll("*-1\r\n") catch {};
    }
}

fn acceptConnection(server_alloc: std.mem.Allocator, server: *net.Server, clients: *std.AutoHashMap(posix.socket_t, Client), poll_fds: *std.ArrayList(posix.pollfd),) !void {
    const conn = try server.accept();

    try clients.put(conn.stream.handle, .{ .conn = conn });
    try poll_fds.append(server_alloc, .{
        .fd = conn.stream.handle,
        .events = posix.POLL.IN,
        .revents = 0,
    });
    std.log.info("Accepted connection - fd: {d}", .{conn.stream.handle});
}

fn handleClientData(server_alloc: std.mem.Allocator, arena: *std.heap.ArenaAllocator, store: *storage.Store, clients: *std.AutoHashMap(posix.socket_t, Client), blocked: *std.AutoHashMap(posix.socket_t, BlockedClient), poll_fds: *std.ArrayList(posix.pollfd),) !void {
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
                    try wakeBlocked(r.key, store, blocked, server_alloc, command_arena);
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
                    try blocked.put(pfd.fd, .{ .keys = keys, .deadline_ms = deadline_ms, .operation = operation});
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

fn wakeBlocked(
    key: []const u8,
    store: *storage.Store,
    blocked: *std.AutoHashMap(posix.socket_t, BlockedClient),
    server_alloc: std.mem.Allocator,
    command_arena: std.mem.Allocator,
) !void {
    var it = blocked.iterator();

    while (it.next()) |e| {
        for (e.value_ptr.keys) |k| {
            if (!std.mem.eql(u8, k, key)) continue;

            const fd = e.key_ptr.*;
            var entry = blocked.fetchRemove(fd).?;
            defer entry.value.deinit(server_alloc);

            const resp = switch (entry.value.operation) {
                .blpop => blk: {
                    const popped = try store.lpop(command_arena, key, 1) orelse return;
                    const resp_items = try command_arena.alloc(protocol.RespValue, 2);
                    resp_items[0] = .{ .bulk_string = key };
                    resp_items[1] = .{ .bulk_string = popped[0] };
                    break :blk protocol.RespValue{ .array = resp_items };
                },
                .xread => |r| blk: {
                    var response: std.ArrayList(protocol.RespValue) = .empty;
                    var has_results = false;
                    for (entry.value.keys, r.ids) |key_str, id_str| {
                        const range_slice = store.streamQuery(key_str, id_str, "+", true) orelse continue;
                        has_results = true; 
                        const range_array = try protocol.assembleStreamResp(command_arena, range_slice);
                        const key_entry = try command_arena.alloc(protocol.RespValue, 2);
                        key_entry[0] = .{ .bulk_string = key_str };
                        key_entry[1] = .{ .array = range_array };
                        try response.append(command_arena, .{ .array = key_entry });
                    }
                    break :blk protocol.RespValue{ .array = try response.toOwnedSlice(command_arena) }; 
                },
            };

            const s = net.Stream{ .handle = fd };
            var w = s.writer(&.{});
            try protocol.serialize(&w.interface, &resp);
            return; // FIFO
        }
    }
}
