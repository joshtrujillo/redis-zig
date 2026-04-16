const std = @import("std");

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

    // GET - get the value at the given key
    // returns null if key does not exist or expired
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

    // SET - set the given value at the given key. Optionally set expires_at.
    pub fn set(
        self: *Store,
        key: []const u8,
        value: []const u8,
        expires_at: ?i64,
    ) !void {
        const owned_key = try self.alloc.dupe(u8, key);
        const owned_value = try self.alloc.dupe(u8, value);
        try self.map.put(
            owned_key,
            .{ .value = .{ .string = owned_value }, .expires_at = expires_at },
        );
    }

    // RPUSH - push a value on the right side of a list at given key
    pub fn rpush(self: *Store, key: []const u8, value: []const u8) !usize {
        const owned_value = try self.alloc.dupe(u8, value);

        if (self.map.getPtr(key)) |entry| {
            switch (entry.value) {
                .list => |*l| {
                    try l.append(owned_value);
                    return l.len;
                },
                else => {
                    self.alloc.free(owned_value);
                    return error.WrongType;
                },
            }
        } else {
            const owned_key = try self.alloc.dupe(u8, key);
            var list = List.init(self.alloc);
            try list.append(owned_value);
            try self.map.put(
                owned_key,
                .{ .value = .{ .list = list }, .expires_at = null },
            );
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
                else => {
                    self.alloc.free(owned_value);
                    return error.WrongType;
                },
            }
        } else {
            const owned_key = try self.alloc.dupe(u8, key);
            var list = List.init(self.alloc);
            try list.prepend(owned_value);
            try self.map.put(
                owned_key,
                .{ .value = .{ .list = list }, .expires_at = null },
            );
            return 1;
        }
    }

    pub fn lrange(
        self: *Store,
        dest_alloc: std.mem.Allocator,
        key: []const u8,
        start: i64,
        stop: i64,
    ) !?[][]const u8 {
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

        var node = list.list.first;
        for (0..s) |_| node = (node orelse break).next;

        const result = try dest_alloc.alloc([]const u8, count);
        for (result) |*slot| {
            const n = node orelse break;
            const item: *ListItem = @fieldParentPtr("node", n);
            slot.* = item.data;
            node = n.next;
        }
        return result;
    }

    pub fn llen(self: *Store, key: []const u8) usize {
        const entry = self.map.getPtr(key) orelse return 0;
        return switch (entry.value) {
            .list => |*l| l.len,
            else => 0,
        };
    }

    pub fn lpop(
        self: *Store,
        dest_alloc: std.mem.Allocator,
        key: []const u8,
        count: usize,
    ) !?[][]const u8 {
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

    pub fn xadd(
        self: *Store,
        key: []const u8,
        id: []const u8,
        args: [][]const u8,
    ) !RecordId {
        const entry = try self.map.getOrPut(key);
        if (!entry.found_existing) {
            entry.key_ptr.* = try self.alloc.dupe(u8, key);
            entry.value_ptr.* = .{
                .value = .{ .stream = Stream.init() },
                .expires_at = null,
            };
        } else if (entry.value_ptr.value != .stream) {
            return error.WrongType;
        }
        const stream = &entry.value_ptr.value.stream;

        const record_id = try resolveId(id, stream.last_id);
        if (record_id.ms == 0 and record_id.sequence == 0) return error.MinId;
        if (!(record_id.order(stream.last_id) == .gt)) return error.InvalidId;

        const owned_fields = try self.alloc.alloc([]const u8, args.len);
        for (args, owned_fields) |src, *dst| dst.* = try self.alloc.dupe(u8, src);

        try stream.entries.append(
            self.alloc,
            .{ .id = record_id, .fields = owned_fields },
        );
        stream.last_id = record_id;
        return record_id;
    }

    pub fn streamQuery(
        self: *Store,
        key: []const u8,
        start_id_raw: []const u8,
        end_id_raw: []const u8,
        exclusive_start: bool,
    ) ?[]const StreamRecord {
        const entry = self.map.getPtr(key) orelse return null;
        const items = switch (entry.value) {
            .stream => |*s| s.entries.items,
            else => return null,
        };
        const start_id = RecordId.parseId(start_id_raw) catch return null;
        const lower = getLowerBound(items, start_id, exclusive_start);
        const end_id = RecordId.parseId(end_id_raw) catch return null;
        const max_int = std.math.maxInt(u64);
        if (end_id.sequence == max_int and end_id.ms == max_int) return items[lower..];
        const upper = std.sort.lowerBound(StreamRecord, items, end_id, compareRecordId);
        return items[lower .. upper + 1];
    }
    
    fn getLowerBound(items: []StreamRecord, start_id: RecordId, exclusive_start: bool) usize {
        const lower_bound = std.sort.lowerBound(StreamRecord, items, start_id, compareRecordId);
        if (exclusive_start and (items[lower_bound].id.order(start_id) == .eq)) {
            return lower_bound + 1;
        } else {
            return lower_bound;
        }
    } 

    fn resolveId(raw_id: []const u8, last_id: RecordId) !RecordId {
        var it = std.mem.splitSequence(u8, raw_id, "-");
        const ms_str = it.next().?;
        const seq_str = it.next() orelse "*";

        const ms: u64 = if (std.mem.eql(u8, ms_str, "*"))
            @intCast(std.time.milliTimestamp())
        else
            try std.fmt.parseInt(u64, ms_str, 10);

        const seq = if (std.mem.eql(u8, seq_str, "*"))
            if (ms == last_id.ms) last_id.sequence + 1 else 0
        else
            try std.fmt.parseInt(u64, seq_str, 10);

        return .{ .ms = ms, .sequence = seq };
    }
};

pub const RecordId = struct {
    ms: u64,
    sequence: u64,

    pub fn parseId(raw_id: []const u8) !RecordId {
        if (std.mem.eql(u8, raw_id, "-"))
            return .{ .ms = 0, .sequence = 0 }
        else if (std.mem.eql(u8, raw_id, "+"))
            return .{
                .ms = std.math.maxInt(u64),
                .sequence = std.math.maxInt(u64),
            };
        var it = std.mem.splitSequence(u8, raw_id, "-");
        const ms_str = it.next() orelse return error.InvalidId;
        const seq_str = it.next() orelse return error.InvalidId;
        return .{
            .ms = try std.fmt.parseInt(u64, ms_str, 10),
            .sequence = try std.fmt.parseInt(u64, seq_str, 10),
        };
    }

    fn order(self: RecordId, other: RecordId) std.math.Order {
        if (self.ms != other.ms) return std.math.order(self.ms, other.ms);
        return std.math.order(self.sequence, other.sequence);
    }
};

pub const StreamRecord = struct {
    id: RecordId,
    fields: [][]const u8,
};

fn compareRecordId(target: RecordId, record: StreamRecord) std.math.Order {
    return target.order(record.id);
}

const Entry = struct {
    value: Value,
    expires_at: ?i64, // null is no expiry
};

const Value = union(enum) {
    string: []const u8,
    list: List,
    stream: Stream,
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
