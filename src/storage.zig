const std = @import("std");

pub const Side = enum { left, right };

// Main storage struct that uses two maps:
// - values: maps keys to their Value (string, list, or stream)
// - expiry: maps keys to their expiration timestamp (only for keys with TTL)
// The expiry map borrows key pointers from the values map.
pub const Store = struct {
    values: std.StringHashMap(Value),
    expiry: std.StringHashMap(i64),
    store_alloc: std.mem.Allocator,

    pub const KeyType = enum { string, list, stream, none };

    pub fn init(alloc: std.mem.Allocator) Store {
        return .{
            .store_alloc = alloc,
            .values = std.StringHashMap(Value).init(alloc),
            .expiry = std.StringHashMap(i64).init(alloc),
        };
    }

    pub fn deinit(self: *Store) void {
        var it = self.values.iterator();
        while (it.next()) |entry| {
            self.freeValue(entry.value_ptr);
            self.store_alloc.free(entry.key_ptr.*);
        }
        self.values.deinit();
        self.expiry.deinit();
    }

    pub fn get(self: *Store, key: []const u8) ?[]const u8 {
        if (self.isExpired(key)) self.remove(key);
        const value = self.values.getPtr(key) orelse return null;
        return switch (value.*) {
            .string => |s| s,
            else => null,
        };
    }

    pub fn set(
        self: *Store,
        key: []const u8,
        value: []const u8,
        expires_at: ?i64,
    ) !void {
        const record = try self.values.getOrPut(key);
        if (record.found_existing) {
            self.freeValue(record.value_ptr);
        } else {
            record.key_ptr.* = try self.store_alloc.dupe(u8, key);
        }
        record.value_ptr.* = .{ .string = try self.store_alloc.dupe(u8, value) };

        if (expires_at) |exp| {
            try self.expiry.put(record.key_ptr.*, exp);
        } else {
            _ = self.expiry.remove(key);
        }
    }

    pub fn push(
        self: *Store,
        key: []const u8,
        value: []const u8,
        side: Side,
    ) !usize {
        const owned_value = try self.store_alloc.dupe(u8, value);

        if (self.values.getPtr(key)) |v| {
            switch (v.*) {
                .list => |*l| {
                    try l.insert(owned_value, side);
                    return l.len;
                },
                else => {
                    self.store_alloc.free(owned_value);
                    return error.WrongType;
                },
            }
        } else {
            const owned_key = try self.store_alloc.dupe(u8, key);
            var list = List.init(self.store_alloc);
            try list.insert(owned_value, side);
            try self.values.put(owned_key, .{ .list = list });
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
        const list = self.getList(key) orelse return null;
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
        return (self.getList(key) orelse return 0).len;
    }

    pub fn lpop(
        self: *Store,
        dest_alloc: std.mem.Allocator,
        key: []const u8,
        count: usize,
    ) !?[][]const u8 {
        const list = self.getList(key) orelse return null;
        const actual = @min(count, list.len);
        if (actual == 0) return null;

        const result = try dest_alloc.alloc([]const u8, actual);
        for (result) |*slot| {
            const node = list.list.popFirst().?;
            list.len -= 1;
            const item: *ListItem = @fieldParentPtr("node", node);
            slot.* = try dest_alloc.dupe(u8, item.data);
            self.store_alloc.free(item.data);
            self.store_alloc.destroy(item);
        }
        return result;
    }

    pub fn typeOf(self: *Store, key: []const u8) KeyType {
        const value = self.values.getPtr(key) orelse return .none;
        return std.meta.stringToEnum(KeyType, @tagName(value.*)) orelse .none;
    }

    pub fn xadd(
        self: *Store,
        key: []const u8,
        id: []const u8,
        args: [][]const u8,
    ) !RecordId {
        const entry = try self.values.getOrPut(key);
        if (!entry.found_existing) {
            entry.key_ptr.* = try self.store_alloc.dupe(u8, key);
            entry.value_ptr.* = .{ .stream = Stream.init() };
        } else if (entry.value_ptr.* != .stream) {
            return error.WrongType;
        }
        const stream = &entry.value_ptr.stream;

        const record_id = try resolveId(id, stream.last_id);
        if (record_id.ms == 0 and record_id.sequence == 0) return error.MinId;
        if (!(record_id.order(stream.last_id) == .gt)) return error.InvalidId;

        const owned_fields = try self.store_alloc.alloc([]const u8, args.len);
        for (args, owned_fields) |src, *dst| dst.* = try self.store_alloc.dupe(u8, src);

        try stream.entries.append(
            self.store_alloc,
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
        const stream = self.getStream(key) orelse return null;
        const items = stream.entries.items;
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

    fn getList(self: *Store, key: []const u8) ?*List {
        const value = self.values.getPtr(key) orelse return null;
        return switch (value.*) {
            .list => |*l| l,
            else => null,
        };
    }

    fn getStream(self: *Store, key: []const u8) ?*Stream {
        const value = self.values.getPtr(key) orelse return null;
        return switch (value.*) {
            .stream => |*s| s,
            else => null,
        };
    }

    fn isExpired(self: *Store, key: []const u8) bool {
        const exp = self.expiry.get(key) orelse return false;
        return std.time.milliTimestamp() >= exp;
    }

    // Removes a key from both maps, freeing the owned key and value.
    fn remove(self: *Store, key: []const u8) void {
        _ = self.expiry.remove(key);
        if (self.values.fetchRemove(key)) |kv| {
            var v = kv.value;
            self.freeValue(&v);
            self.store_alloc.free(kv.key);
        }
    }

    fn freeValue(self: *Store, value: *Value) void {
        switch (value.*) {
            .string => |s| self.store_alloc.free(s),
            .list => |*l| l.deinit(),
            .stream => |*s| s.deinit(self.store_alloc),
        }
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

    pub fn toStr(self: RecordId, alloc: std.mem.Allocator) ![]const u8 {
        return try std.fmt.allocPrint(alloc, "{d}-{d}", .{ self.ms, self.sequence });
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

    fn insert(self: *List, data: []const u8, side: Side) !void {
        const item = try self.alloc.create(ListItem);
        item.* = .{ .data = data };
        if (side == .left)
            self.list.prepend(&item.node)
        else
            self.list.append(&item.node);
        self.len += 1;
    }
};
