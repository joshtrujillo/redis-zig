const std = @import("std");
const posix = std.posix;
const protocol = @import("protocol.zig");
const storage = @import("storage.zig");

pub const BlockedClient = struct {
    deadline_ms: ?i64,
    keys: [][]const u8,
    operation: protocol.BlockedOp = .{ .blpop = {} },

    pub fn deinit(self: *BlockedClient, alloc: std.mem.Allocator) void {
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

pub const WakeResult = struct {
    fd: posix.socket_t,
    response: protocol.RespValue,
};

pub fn computePollTimeout(blocked: *std.AutoHashMap(posix.socket_t, BlockedClient)) i32 {
    var poll_timeout_ms: i32 = -1;
    const now = std.time.milliTimestamp();
    var it = blocked.iterator();
    while (it.next()) |e| {
        if (e.value_ptr.deadline_ms) |dl| {
            const ms: i32 = @intCast(@max(0, @min(dl - now, std.math.maxInt(i32))));
            if (poll_timeout_ms == -1 or ms < poll_timeout_ms) poll_timeout_ms = ms;
        }
    }
    return poll_timeout_ms;
}

/// Returns list of expired fds. Removes them from blocked and calls deinit.
/// Caller is responsible for sending null responses to these fds.
pub fn expireBlocked(
    alloc: std.mem.Allocator,
    blocked: *std.AutoHashMap(posix.socket_t, BlockedClient),
) ![]posix.socket_t {
    const now_ms = std.time.milliTimestamp();
    var expired: std.ArrayList(posix.socket_t) = .empty;
    var it = blocked.iterator();
    while (it.next()) |e| {
        if (e.value_ptr.deadline_ms) |dl_ms|
            if (now_ms >= dl_ms) try expired.append(alloc, e.key_ptr.*);
    }
    for (expired.items) |fd| {
        var entry = blocked.fetchRemove(fd).?;
        entry.value.deinit(alloc);
    }
    return try expired.toOwnedSlice(alloc);
}

/// Find the first blocked client waiting on `key`, compute its response,
/// remove it from `blocked`, and return what to send.
pub fn resolveWake(
    key: []const u8,
    store: *storage.Store,
    blocked: *std.AutoHashMap(posix.socket_t, BlockedClient),
    server_alloc: std.mem.Allocator,
    arena: std.mem.Allocator,
) !?WakeResult {
    var it = blocked.iterator();

    while (it.next()) |e| {
        for (e.value_ptr.keys) |k| {
            if (!std.mem.eql(u8, k, key)) continue;

            const fd = e.key_ptr.*;
            var entry = blocked.fetchRemove(fd).?;
            defer entry.value.deinit(server_alloc);

            const resp: protocol.RespValue = switch (entry.value.operation) {
                .blpop => blk: {
                    const popped = try store.lpop(arena, key, 1) orelse return null;
                    const resp_items = try arena.alloc(protocol.RespValue, 2);
                    resp_items[0] = .{ .bulk_string = key };
                    resp_items[1] = .{ .bulk_string = popped[0] };
                    break :blk .{ .array = resp_items };
                },
                .xread => |r| blk: {
                    var response: std.ArrayList(protocol.RespValue) = .empty;
                    for (entry.value.keys, r.ids) |key_str, id_str| {
                        const range_slice = store.streamQuery(key_str, id_str, "+", true) orelse continue;
                        const range_array = try protocol.assembleStreamResp(arena, range_slice);
                        const key_entry = try arena.alloc(protocol.RespValue, 2);
                        // Dupe key_str into arena since entry.value.deinit will free the originals
                        key_entry[0] = .{ .bulk_string = try arena.dupe(u8, key_str) };
                        key_entry[1] = .{ .array = range_array };
                        try response.append(arena, .{ .array = key_entry });
                    }
                    break :blk .{ .array = try response.toOwnedSlice(arena) };
                },
            };

            return .{ .fd = fd, .response = resp };
        }
    }
    return null;
}

// Tests

test "computePollTimeout: returns -1 with no blocked clients" {
    var blocked = std.AutoHashMap(posix.socket_t, BlockedClient).init(std.testing.allocator);
    defer blocked.deinit();
    try std.testing.expectEqual(@as(i32, -1), computePollTimeout(&blocked));
}

test "computePollTimeout: returns -1 when deadline is null (block forever)" {
    var blocked = std.AutoHashMap(posix.socket_t, BlockedClient).init(std.testing.allocator);
    defer blocked.deinit();
    const keys = try std.testing.allocator.alloc([]const u8, 1);
    keys[0] = try std.testing.allocator.dupe(u8, "k");
    try blocked.put(10, .{ .keys = keys, .deadline_ms = null });
    defer {
        var entry = blocked.fetchRemove(10).?;
        entry.value.deinit(std.testing.allocator);
    }
    try std.testing.expectEqual(@as(i32, -1), computePollTimeout(&blocked));
}

test "resolveWake: blpop wakes blocked client and returns response" {
    var store = storage.Store.init(std.testing.allocator);
    defer store.deinit();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var blocked = std.AutoHashMap(posix.socket_t, BlockedClient).init(std.testing.allocator);
    defer blocked.deinit();

    // Push data into the list
    _ = try store.push("mylist", "hello", .right);

    // Register a blocked client on fd 42
    const keys = try std.testing.allocator.alloc([]const u8, 1);
    keys[0] = try std.testing.allocator.dupe(u8, "mylist");
    try blocked.put(42, .{ .keys = keys, .deadline_ms = null });

    const result = try resolveWake("mylist", &store, &blocked, std.testing.allocator, arena.allocator());
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(posix.socket_t, 42), result.?.fd);
    // Client should be removed from blocked
    try std.testing.expectEqual(@as(u32, 0), blocked.count());

    // Verify the response shape: array of [key, value]
    const arr = result.?.response.array;
    try std.testing.expectEqual(@as(usize, 2), arr.len);
    try std.testing.expectEqualStrings("mylist", arr[0].bulk_string);
    try std.testing.expectEqualStrings("hello", arr[1].bulk_string);
}

test "resolveWake: xread wakes blocked client with new stream entries" {
    var store = storage.Store.init(std.testing.allocator);
    defer store.deinit();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var blocked = std.AutoHashMap(posix.socket_t, BlockedClient).init(std.testing.allocator);
    defer blocked.deinit();

    // Add stream entry
    var fields = [_][]const u8{ "temp", "42" };
    _ = try store.xadd("mystream", "1-0", &fields);

    // Register blocked XREAD client on fd 7, waiting from 0-0
    const keys = try std.testing.allocator.alloc([]const u8, 1);
    keys[0] = try std.testing.allocator.dupe(u8, "mystream");
    const ids = try std.testing.allocator.alloc([]const u8, 1);
    ids[0] = try std.testing.allocator.dupe(u8, "0-0");
    try blocked.put(7, .{
        .keys = keys,
        .deadline_ms = null,
        .operation = .{ .xread = .{ .ids = ids } },
    });

    const result = try resolveWake("mystream", &store, &blocked, std.testing.allocator, arena.allocator());
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(posix.socket_t, 7), result.?.fd);
    try std.testing.expectEqual(@as(u32, 0), blocked.count());

    // Outer array: one stream entry
    const outer = result.?.response.array;
    try std.testing.expectEqual(@as(usize, 1), outer.len);
    // Inner: [key, entries_array]
    const stream_entry = outer[0].array;
    try std.testing.expectEqualStrings("mystream", stream_entry[0].bulk_string);
}

test "resolveWake: returns null when no blocked client matches key" {
    var store = storage.Store.init(std.testing.allocator);
    defer store.deinit();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var blocked = std.AutoHashMap(posix.socket_t, BlockedClient).init(std.testing.allocator);
    defer blocked.deinit();

    const result = try resolveWake("nokey", &store, &blocked, std.testing.allocator, arena.allocator());
    try std.testing.expect(result == null);
}

test "resolveWake: only wakes first matching client (FIFO)" {
    var store = storage.Store.init(std.testing.allocator);
    defer store.deinit();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var blocked = std.AutoHashMap(posix.socket_t, BlockedClient).init(std.testing.allocator);
    defer {
        var it = blocked.valueIterator();
        while (it.next()) |b| b.deinit(std.testing.allocator);
        blocked.deinit();
    }

    _ = try store.push("q", "val1", .right);
    _ = try store.push("q", "val2", .right);

    // Two clients blocked on the same key
    for ([_]posix.socket_t{ 10, 20 }) |fd| {
        const keys = try std.testing.allocator.alloc([]const u8, 1);
        keys[0] = try std.testing.allocator.dupe(u8, "q");
        try blocked.put(fd, .{ .keys = keys, .deadline_ms = null });
    }

    // First call should wake one, leaving one blocked
    const r1 = try resolveWake("q", &store, &blocked, std.testing.allocator, arena.allocator());
    try std.testing.expect(r1 != null);
    try std.testing.expectEqual(@as(u32, 1), blocked.count());
}
