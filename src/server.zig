// src/server.zig

const std = @import("std");
const net = std.net;
const posix = std.posix;
const el = @import("event_loop.zig");
const netx = @import("network.zig");
const protocol = @import("protocol.zig");
const storage = @import("storage.zig");
const engine = @import("engine.zig");

pub const BlockedClient = struct {
    deadline_ms: ?i64,
    keys: [][]const u8,
    operation: engine.BlockedOp = .{ .blpop = {} },

    pub fn deinit(self: *BlockedClient, alloc: std.mem.Allocator) void {
        for (self.keys) |k| alloc.free(k);
        alloc.free(self.keys);
        switch (self.operation) {
            .xread => |x| alloc.free(x.ids),
            .blpop => {},
        }
        self.* = undefined;
    }
};

pub const Client = struct {
    conn: netx.Connection,
    parser: protocol.Parser = .{},
    queued_commands: ?std.ArrayList(protocol.RespValue) = null,

    pub fn deinit(self: *Client, alloc: std.mem.Allocator) void {
        if (self.queued_commands) |*q| {
            for (q.items) |cmd| cmd.free(alloc);
            q.deinit(alloc);
        }
        self.conn.deinit(alloc);
        self.* = undefined;
    }
};

pub const ServerConfig = struct {
    port: u16 = 6379,
    role: []const u8 = "master",
    replica_of: ?[]const u8 = null,
};

pub const Server = struct {
    alloc: std.mem.Allocator,
    store: storage.Store,
    clients: std.AutoHashMap(posix.socket_t, Client),
    blocked: std.AutoHashMap(posix.socket_t, BlockedClient),
    reactor: el.Reactor(el.PollBackend),
    listener: net.Server,
    config: ServerConfig,

    pub fn init(alloc: std.mem.Allocator, config: ServerConfig) !Server {
        const address = try net.Address.resolveIp("127.0.0.1", config.port);
        var srv = Server{
            .alloc = alloc,
            .store = storage.Store.init(alloc),
            .clients = std.AutoHashMap(posix.socket_t, Client).init(alloc),
            .blocked = std.AutoHashMap(posix.socket_t, BlockedClient).init(alloc),
            .reactor = try el.Reactor(el.PollBackend).init(alloc),
            .listener = try address.listen(.{ .reuse_address = true }),
            .config = config,
        };
        try srv.reactor.register(srv.listener.stream.handle);
        return srv;
    }

    pub fn deinit(self: *Server) void {
        var cit = self.clients.valueIterator();
        while (cit.next()) |c| c.deinit(self.alloc);
        self.clients.deinit();
        var bit = self.blocked.valueIterator();
        while (bit.next()) |b| b.deinit(self.alloc);
        self.blocked.deinit();
        self.store.deinit();
        self.listener.deinit();
    }

    pub fn acceptClient(self: *Server) !void {
        const conn = try self.listener.accept();
        const client = Client{
            .conn = try netx.Connection.init(conn.stream),
        };
        try self.clients.put(conn.stream.handle, client);
        try self.reactor.register(conn.stream.handle);
        std.log.info("Accepted connection - fd: {d}", .{conn.stream.handle});
    }

    pub fn removeClient(self: *Server, fd: posix.socket_t) void {
        self.reactor.unregister(fd);
        _ = self.blocked.fetchRemove(fd);
        if (self.clients.fetchRemove(fd)) |entry| {
            var c = entry.value;
            c.deinit(self.alloc);
        }
    }

    pub fn expireBlockedClients(self: *Server) void {
        const now_ms = std.time.milliTimestamp();
        var expired_buf: [64]posix.socket_t = undefined;
        var expired_len: usize = 0;
        var it = self.blocked.iterator();
        while (it.next()) |e| {
            if (e.value_ptr.deadline_ms) |dl_ms| {
                if (now_ms >= dl_ms) {
                    if (expired_len >= expired_buf.len) break;
                    expired_buf[expired_len] = e.key_ptr.*;
                    expired_len += 1;
                }
            }
        }
        for (expired_buf[0..expired_len]) |fd| {
            var entry = self.blocked.fetchRemove(fd).?;
            entry.value.deinit(self.alloc);
            if (self.clients.getPtr(fd)) |client| {
                client.conn.queueSend(self.alloc, "*-1\r\n") catch {};
                _ = client.conn.flush() catch {};
            }
        }
    }

    pub fn computeTimeout(self: *Server) i32 {
        var timeout_ms: i32 = -1;
        const now = std.time.milliTimestamp();
        var it = self.blocked.iterator();
        while (it.next()) |e| {
            if (e.value_ptr.deadline_ms) |dl| {
                const ms: i32 = @intCast(@max(0, @min(dl - now, std.math.maxInt(i32))));
                if (timeout_ms == -1 or ms < timeout_ms) timeout_ms = ms;
            }
        }
        return timeout_ms;
    }

    pub fn blockClient(self: *Server, fd: posix.socket_t, info: engine.BlockInfo) !void {
        const keys = try self.alloc.alloc([]const u8, info.keys.len);
        for (info.keys, keys) |src, *dst| dst.* = try self.alloc.dupe(u8, src);
        const operation: engine.BlockedOp = switch (info.operation) {
            .blpop => .{ .blpop = {} },
            .xread => |x| blk: {
                const ids = try self.alloc.alloc(storage.RecordId, x.ids.len);
                @memcpy(ids, x.ids);
                break :blk .{ .xread = .{ .ids = ids } };
            },
        };
        const deadline_ms: ?i64 = if (info.timeout_ms == 0) null else std.time.milliTimestamp() + @as(i64, @intCast(info.timeout_ms));
        try self.blocked.put(fd, .{ .keys = keys, .deadline_ms = deadline_ms, .operation = operation });
    }

    pub fn resolveWake(self: *Server, key: []const u8, arena: std.mem.Allocator) !void {
        var it = self.blocked.iterator();
        while (it.next()) |e| {
            for (e.value_ptr.keys) |k| {
                if (!std.mem.eql(u8, k, key)) continue;

                const fd = e.key_ptr.*;
                var entry = self.blocked.fetchRemove(fd).?;
                defer entry.value.deinit(self.alloc);

                const resp: protocol.RespValue = switch (entry.value.operation) {
                    .blpop => blk: {
                        const popped = try self.store.lpop(arena, key, 1) orelse return;
                        const resp_items = try arena.alloc(protocol.RespValue, 2);
                        resp_items[0] = .{ .bulk_string = key };
                        resp_items[1] = .{ .bulk_string = popped[0] };
                        break :blk .{ .array = resp_items };
                    },
                    .xread => |r| blk: {
                        var response: std.ArrayList(protocol.RespValue) = .empty;
                        for (entry.value.keys, r.ids) |key_str, id| {
                            const range_slice = self.store.streamQueryFrom(key_str, id) orelse continue;
                            if (range_slice.len == 0) continue;
                            const range_array = try engine.assembleStreamResp(arena, range_slice);
                            const key_entry = try arena.alloc(protocol.RespValue, 2);
                            key_entry[0] = .{ .bulk_string = try arena.dupe(u8, key_str) };
                            key_entry[1] = .{ .array = range_array };
                            try response.append(arena, .{ .array = key_entry });
                        }
                        break :blk .{ .array = try response.toOwnedSlice(arena) };
                    },
                };

                if (self.clients.getPtr(fd)) |wake_client| {
                    try self.sendReply(wake_client, &resp);
                    _ = try wake_client.conn.flush();
                }
                return;
            }
        }
    }

    pub fn dispatch(self: *Server, fd: posix.socket_t, value: protocol.RespValue, arena: std.mem.Allocator) !void {
        const client = self.clients.getPtr(fd) orelse return;
        const cmd_name = value.array[0].bulk_string;

        // Handle MULTI, EXEC, and DISCARD
        if (client.queued_commands) |*queue| {
            if (std.ascii.eqlIgnoreCase(cmd_name, "EXEC")) {
                defer {
                    for (queue.items) |cmd| cmd.free(self.alloc);
                    queue.deinit(self.alloc);
                    client.queued_commands = null;
                }
                const replies = try arena.alloc(protocol.RespValue, queue.items.len);
                for (queue.items, replies) |cmd, *reply_slot| {
                    reply_slot.* = switch (try engine.execute(arena, &self.store, cmd)) {
                        .reply => |r| r,
                        .reply_and_wake => |r| blk: {
                            try self.resolveWake(r.wake_key, arena);
                            break :blk r.reply;
                        },
                        .block => .{ .null_value = {} },
                    };
                }
                return self.sendReply(client, &.{ .array = replies });
            }
            if (std.ascii.eqlIgnoreCase(cmd_name, "DISCARD")) {
                for (queue.items) |cmd| cmd.free(self.alloc);
                queue.deinit(self.alloc);
                client.queued_commands = null;
                return self.sendReply(client, &.{ .simple_string = "OK" });
            }
            if (std.ascii.eqlIgnoreCase(cmd_name, "MULTI")) {
                return self.sendReply(client, &.{ .error_msg = "MULTI calls can not be nested" });
            }
            // queue the command, reply +QUEUED
            const owned = try value.dupe(self.alloc);
            try queue.append(self.alloc, owned);
            return self.sendReply(client, &.{ .simple_string = "QUEUED" });
        }

        // Enter MULTI
        if (std.ascii.eqlIgnoreCase(cmd_name, "MULTI")) {
            client.queued_commands = .empty;
            return self.sendReply(client, &.{ .simple_string = "OK" });
        }

        if (std.ascii.eqlIgnoreCase(cmd_name, "EXEC")) {
            return self.sendReply(client, &.{ .error_msg = "EXEC without MULTI" });
        }

        if (std.ascii.eqlIgnoreCase(cmd_name, "DISCARD")) {
            return self.sendReply(client, &.{ .error_msg = "DISCARD without MULTI" });
        }
        
        if (std.ascii.eqlIgnoreCase(cmd_name, "INFO")) {
            const info_str = try std.fmt.allocPrint(arena, "role:{s}", .{self.config.role});
            return self.sendReply(client, &.{ .bulk_string = info_str });
        }

        // Normal execution
        try self.executeAndApply(client, fd, value, arena);
    }

    fn executeAndApply(self: *Server, client: *Client, fd: posix.socket_t, value: protocol.RespValue, arena: std.mem.Allocator) !void {
        switch (try engine.execute(arena, &self.store, value)) {
            .reply => |r| try self.sendReply(client, &r),
            .reply_and_wake => |r| {
                try self.sendReply(client, &r.reply); std.log.info("wake: key={s} blocked_count={d}", .{ r.wake_key, self.blocked.count() });
                try self.resolveWake(r.wake_key, arena);
            },
            .block => |b| try self.blockClient(fd, b),
        }
    }

    fn sendReply(self: *Server, client: *Client, reply: *const protocol.RespValue) !void {
        var w: std.io.Writer.Allocating = .fromArrayList(self.alloc, &client.conn.send_buf);
        try protocol.serialize(&w.writer, reply);
        client.conn.send_buf = w.toArrayList();
    }
};
