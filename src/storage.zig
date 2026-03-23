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
            self.alloc.free(entry.value_ptr.*.value);
        }
        self.map.deinit();
    }

    pub fn get(self: *Store, key: []const u8) ?[]const u8 {
        const entry = self.map.get(key) orelse return null;
        if (entry.expires_at) |exp| {
            if (std.time.milliTimestamp() >= exp) return null;
        }
        return entry.value;
    }

    pub fn set(self: *Store, key: []const u8, value: []const u8, expires_at: ?i64) !void {
        const owned_key = try self.alloc.dupe(u8, key);
        const owned_value = try self.alloc.dupe(u8, value);
        try self.map.put(owned_key, .{ .value = owned_value, .expires_at = expires_at });
    }
};

const Entry = struct {
    value: []const u8,
    expires_at: ?i64, // null is no expiry
};
