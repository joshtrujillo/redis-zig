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

    fn isGreater(self: *const RecordId, id: RecordId) bool {
        return (self.ms > id.ms or (self.ms == id.ms and self.sequence > id.sequence));
    }
};

const StreamRecord = struct {
    id: RecordId,
    fields: [][]const u8,
};

const Stream = struct {
    entries: std.ArrayList(StreamRecord),
    last_id: RecordId,

    fn init() Stream {
        return .{ .entries = .{}, .last_id = .{ .ms = 0, .sequence = 0 } };
    }

    fn deinit(self: *Stream, alloc: std.mem.Allocator) void {
        for (self.entries.items) |record| {
            for (record.fields) |f| alloc.free(f);
            alloc.free(record.fields);
        }
        self.entries.deinit(alloc);
    }
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

    fn deinit(self: *List) void {
        var it = self.list.first;
        while (it) |node| {
            it = node.next;
            const item: *ListItem = @fieldParentPtr("node", node);
            self.alloc.free(item.data);
            self.alloc.destroy(item);
        }
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
                .stream => |*stream| stream.deinit(self.alloc),
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
            else => return null,
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
                else => return error.WrongType,
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
                else => return error.WrongType,
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
            .list => |*l| l,
            else => return null,
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
            .list => |*l| l.len,
            else => 0,
        };
    }

    pub fn lpop(self: *Store, dest_alloc: std.mem.Allocator, key: []const u8, count: usize) !?[][]const u8 {
        const entry = self.map.getPtr(key) orelse return null;
        const list = switch (entry.value) {
            .list => |*l| l,
            else => return null,
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

    pub fn xadd(self: *Store, key: []const u8, id: []const u8, args: [][]const u8) ![]const u8 {
        const owned_key = try self.alloc.dupe(u8, key);
        const entry = try self.map.getOrPut(owned_key);
        if (!entry.found_existing) {
            entry.value_ptr.* = .{ .value = .{ .stream = Stream.init() }, .expires_at = null };
        } else if (entry.value_ptr.value != .stream) {
            return error.WrongType;
        }
        const stream = &entry.value_ptr.value.stream;
        const owned_fields = try self.alloc.alloc([]const u8, args.len);
        for (args, owned_fields) |src, *dst| dst.* = try self.alloc.dupe(u8, src);

        var it = std.mem.splitSequence(u8, id, "-");
        const ms = try std.fmt.parseInt(u64, it.next().?, 10);
        const seq = try std.fmt.parseInt(u64, it.next().?, 10);
        const recordId = RecordId{ .ms = ms, .sequence = seq };
        if (ms == 0 and seq == 0) return error.MinId;
        if (!recordId.isGreater(stream.last_id)) return error.InvalidId;

        try stream.entries.append(
            self.alloc,
            .{ .id = .{ .ms = ms, .sequence = seq }, .fields = owned_fields },
        );
        stream.last_id = recordId;
        return id;
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
