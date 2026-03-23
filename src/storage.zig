const std = @import("std");

pub const Store = struct {
    map: std.StringHashMap([]const u8),
    alloc: std.mem.Allocator,

    pub fn init(alloc: std.mem.Allocator) Store {
        return .{
            .alloc = alloc,
            .map = std.StringHashMap([]const u8).init(alloc),
        };
    }

    pub fn deinit(self: *Store) void {
        var it = self.map.iterator();
        while (it.next()) |entry| {
            self.alloc.free(entry.key_ptr.*);
            self.alloc.free(entry.value_ptr.*);
        }
        self.map.deinit();
    }

    pub fn get(self: *Store, key: []const u8) ?[]const u8 {
        return self.map.get(key);
    }

    pub fn set(self: *Store, key: []const u8, value: []const u8) !void {
        const owned_key = try self.alloc.dupe(u8, key);
        const owned_value = try self.alloc.dupe(u8, value);
        try self.map.put(owned_key, owned_value);
    }
};
