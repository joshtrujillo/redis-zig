const std = @import("std");

const Entry = struct {
    value: Value,
    expires_at: ?i64, // null is no expiry
};

const Value = union(enum) {
    string: []const u8,
    list: List,
};

const ListItem = struct {
    data: []const u8,
    node: std.DoublyLinkedList.Node = .{},
};

const List = struct {
    list: std.DoublyLinkedList = .{},
    len: usize = 0,

    fn append(self: *List, alloc: std.mem.Allocator, data: []const u8) !void {
        const item = try alloc.create(ListItem);
        item.* = .{ .data = data };
        self.list.append(&item.node);
        self.len += 1;
    }

    fn prepend(self: *List, alloc: std.mem.Allocator, data: []const u8) !void {
        const item = try alloc.create(ListItem);
        item.* = .{ .data = data };
        self.list.prepend(&item.node);
        self.len += 1;
    }

    fn deinit(self: *List, alloc: std.mem.Allocator) void {
        var it = self.list.first;
        while (it) |node| {
            it = node.next;
            const item: *ListItem = @fieldParentPtr("node", node);
            alloc.free(item.data);
            alloc.destroy(item);
        }
    }
};

// Main storage struct that utilizes a []const u8 -> Entry hash map.
// The methods for Store implement the RESP commands, and are responsible
// for dealing with the actual storage of the data in memory.
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
                .list => |*list| {
                    var l = list;
                    l.deinit(self.alloc);
                },
            }
        }
        self.map.deinit();
    }

    pub fn get(self: *Store, key: []const u8) ?[]const u8 {
        const entry = self.map.getPtr(key) orelse return null;
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
                    return list.len;
                },
                .string => return error.WrongType,
            }
        } else {
            const owned_key = try self.alloc.dupe(u8, key);
            var list: List = .{};
            try list.append(self.alloc, owned_value);
            try self.map.put(owned_key, .{ .value = .{ .list = list }, .expires_at = null });
            return 1;
        }
    }

    pub fn lpush(self: *Store, key: []const u8, value: []const u8) !usize {
        const owned_value = try self.alloc.dupe(u8, value);

        if (self.map.getPtr(key)) |entry| {
            switch (entry.value) {
                .list => |*list| {
                    try list.prepend(self.alloc, owned_value);
                    return list.len;
                },
                .string => return error.WrongType,
            }
        } else {
            const owned_key = try self.alloc.dupe(u8, key);
            var list: List = .{};
            try list.prepend(self.alloc, owned_value);
            try self.map.put(owned_key, .{ .value = .{ .list = list }, .expires_at = null });
            return 1;
        }
    }

    pub fn lrange(self: *Store, key: []const u8, start: i64, stop: i64) ?LrangeIterator {
        const entry = self.map.getPtr(key) orelse return null;
        const list = &entry.value.list;
        const list_len: i64 = @intCast(list.len);

        var norm_start: i64 = if (start < 0) list_len + start else start;
        var norm_stop: i64 = if (stop < 0) list_len + stop else stop;

        if (norm_start < 0) norm_start = 0;
        if (norm_stop >= list_len) norm_stop = list_len - 1;

        if (norm_start > norm_stop) return null;

        const s: usize = @intCast(norm_start);
        const count: usize = @intCast(norm_stop - norm_start + 1);

        // Walk to norm_start
        var node = list.list.first;
        for (0..s) |_| node = (node orelse break).next;

        return .{ .current = node, .count = count };
    }

    pub fn llen(self: *Store, key: []const u8) usize {
        const entry = self.map.getPtr(key) orelse return 0;
        return entry.value.list.len;
    }

    pub fn lpop(self: *Store, key: []const u8) ?[]const u8 {
        const entry = self.map.getPtr(key) orelse return null;
        const list = switch (entry.value) {
            .string => return null,
            .list => |*l| l
        };
        const node = list.list.popFirst() orelse return null;
        list.len -= 1;
        const item: *ListItem = @fieldParentPtr("node", node);
        const data = item.data;
        self.alloc.destroy(item);
        return data;
    }
};

pub const LrangeIterator = struct {
    current: ?*std.DoublyLinkedList.Node,
    count: usize,

    pub fn next(self: *LrangeIterator) ?[]const u8 {
        if (self.count == 0) return null;
        const node = self.current orelse return null;
        const item: *ListItem = @fieldParentPtr("node", node);
        self.current = node.next;
        self.count -= 1;
        return item.data;
    }
};
