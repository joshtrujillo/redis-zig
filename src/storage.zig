const std = @import("std");

const Entry = struct {
    value: Value,
    expires_at: ?i64, // null is no expiry
};

const Value = union(enum) {
    string: []const u8,
    list: List,
    stream: Stream,
};

const RecordId = struct {
    ms: u64,
    sequence: u64,
};

const StreamRecord = struct {
    id: RecordId,
    fields: std.StringHashMap([]const u8),
};

const Stream = struct {
    entries: std.ArrayList(StreamRecord),
    last_id: RecordId,
};

const ListItem = struct {
    data: []const u8,
    node: std.DoublyLinkedList.Node = .{},
};

const List = struct {
    list: std.DoublyLinkedList = .{},
    len: usize = 0,
    alloc: std.mem.Allocator,

    fn init(alloc: std.mem.Allocator) List {
        return .{ .alloc = alloc, .list = .{}, .len = 0 };
    }

    fn append(self: *List, data: []const u8) !void {
        const item = try self.alloc.create(ListItem);
        item.* = .{ .data = data };
        self.list.append(&item.node);
        self.len += 1;
    }

    fn prepend(self: *List, data: []const u8) !void {
        const item = try self.alloc.create(ListItem);
        item.* = .{ .data = data };
        self.list.prepend(&item.node);
        self.len += 1;
    }

    fn deinit(self: *List) void {
        var it = self.list.first;
        while (it) |node| {
            it = node.next;
            const item: *ListItem = @fieldParentPtr("node", node);
            self.alloc.free(item.data);
            self.alloc.destroy(item);
        }
    }
};

// Main storage struct that utilizes a []const u8 -> Entry hash map.
// The methods for Store implement the RESP commands, and are responsible
// for dealing with the actual storage of the data in memory.
pub const Store = struct {
    map: std.StringHashMap(Entry),
    alloc: std.mem.Allocator,

    pub const KeyType = enum { string, list, stream, none };

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
                .list => |*list| list.deinit(),
                .stream => {},
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
            .list, .stream => return null,
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
                .list => |*l| {
                    try l.append(owned_value);
                    return l.len;
                },
                .string, .stream => return error.WrongType,
            }
        } else {
            const owned_key = try self.alloc.dupe(u8, key);
            var list = List.init(self.alloc);
            try list.append(owned_value);
            try self.map.put(owned_key, .{ .value = .{ .list = list }, .expires_at = null });
            return 1;
        }
    }

    pub fn lpush(self: *Store, key: []const u8, value: []const u8) !usize {
        const owned_value = try self.alloc.dupe(u8, value);

        if (self.map.getPtr(key)) |entry| {
            switch (entry.value) {
                .list => |*l| {
                    try l.prepend(owned_value);
                    return l.len;
                },
                .string, .stream => return error.WrongType,
            }
        } else {
            const owned_key = try self.alloc.dupe(u8, key);
            var list = List.init(self.alloc);
            try list.prepend(owned_value);
            try self.map.put(owned_key, .{ .value = .{ .list = list }, .expires_at = null });
            return 1;
        }
    }

    pub fn lrange(self: *Store, key: []const u8, start: i64, stop: i64) ?ListIterator {
        const entry = self.map.getPtr(key) orelse return null;
        const list = switch (entry.value) {
            .string, .stream => return null,
            .list => |*l| l,
        };
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
        return switch (entry.value) {
            .string, .stream => 0,
            .list => |*l| l.len,
        };
    }

    pub fn lpop(self: *Store, dest_alloc: std.mem.Allocator, key: []const u8, count: usize) !?[][]const u8 {
        const entry = self.map.getPtr(key) orelse return null;
        const list = switch (entry.value) {
            .string, .stream => return null,
            .list => |*l| l,
        };
        const actual = @min(count, list.len);
        if (actual == 0) return null;

        const result = try dest_alloc.alloc([]const u8, actual);
        for (result) |*slot| {
            const node = list.list.popFirst().?;
            list.len -= 1;
            const item: *ListItem = @fieldParentPtr("node", node);
            slot.* = try dest_alloc.dupe(u8, item.data);
            self.alloc.free(item.data);
            self.alloc.destroy(item);
        }
        return result;
    }

    pub fn typeOf(self: *Store, key: []const u8) KeyType {
        const entry = self.map.getPtr(key) orelse return .none;
        return switch (entry.value) {
            .string => .string,
            .list => .list,
            .stream => .stream,
        };
    }
};

pub const ListIterator = struct {
    current: ?*std.DoublyLinkedList.Node,
    count: usize,

    pub fn next(self: *ListIterator) ?[]const u8 {
        if (self.count == 0) return null;
        const node = self.current orelse return null;
        const item: *ListItem = @fieldParentPtr("node", node);
        self.current = node.next;
        self.count -= 1;
        return item.data;
    }
};
