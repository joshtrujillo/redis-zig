const std = @import("std");

pub const Store = struct {
    map: std.StringHashMap(Entry),
    alloc: std.mem.Allocator,

    pub fn init(alloc: std.mem.Allocator) Store {
        return .{
            .alloc = alloc,
            .map = std.StringHashMap(Entry).init(alloc),
        };
    }

    pub fn deinit(self: *Store) void {
        var it = self.map.iterator();
        while (it.next()) |entry| {
            self.alloc.free(entry.key_ptr.*);
            switch (entry.value_ptr.*.value) {
                .string => |s| self.alloc.free(s),
                .list => |list| {
                    var l = list;
                    for (l.items) |item| self.alloc.free(item);
                    l.deinit(self.alloc);
                },
            }
        }
        self.map.deinit();
    }

    pub fn get(self: *Store, key: []const u8) ?[]const u8 {
        const entry = self.map.get(key) orelse return null;
        if (entry.expires_at) |exp| {
            if (std.time.milliTimestamp() >= exp) {
                _ = self.map.remove(key);
                return null;
            }
        }
        return switch (entry.value) {
            .string => |s| s,
            .list => return null,
        };
    }

    pub fn set(self: *Store, key: []const u8, value: []const u8, expires_at: ?i64) !void {
        const owned_key = try self.alloc.dupe(u8, key);
        const owned_value = try self.alloc.dupe(u8, value);
        try self.map.put(owned_key, .{ .value = .{ .string = owned_value }, .expires_at = expires_at });
    }

    pub fn rpush(self: *Store, key: []const u8, value: []const u8) !usize {
        const owned_value = try self.alloc.dupe(u8, value);

        if (self.map.getPtr(key)) |entry| {
            switch (entry.value) {
                .list => |*list| {
                    try list.append(self.alloc, owned_value);
                    return list.items.len;
                },
                .string => return error.WrongType,
            }
        } else {
            // key doesn't exist; create a new list
            const owned_key = try self.alloc.dupe(u8, key);
            var list: std.ArrayList([]const u8) = .empty;
            try list.append(self.alloc, owned_value);
            try self.map.put(owned_key, .{ .value = .{ .list = list }, .expires_at = null });
            return 1;
        }
    }

    pub fn lrange(self: *Store, key: []const u8, start: i64, stop: i64) ?[][]const u8 {
        const entry = self.map.get(key) orelse return null;
        const list_len: i64 = @intCast(entry.value.list.items.len);

        var norm_start: i64 = if (start < 0) list_len + start else start;
        var norm_stop: i64 = if (stop < 0) list_len + stop else stop;

        if (norm_start < 0) norm_start = 0;
        if (norm_stop >= list_len) norm_stop = list_len - 1;

        if (norm_start > norm_stop) return null;

        const s: usize = @intCast(norm_start);
        const e: usize = @intCast(norm_stop);
        return entry.value.list.items[s .. e + 1];
    }
};

const Entry = struct {
    value: Value,
    expires_at: ?i64, // null is no expiry
};

const Value = union(enum) {
    string: []const u8,
    list: std.ArrayList([]const u8),
};
