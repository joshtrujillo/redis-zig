// src/network.zig

const std = @import("std");
const protocol = @import("protocol.zig");
const RingBuffer = @import("ring_buffer.zig").RingBuffer;
const net = std.net;
const posix = std.posix;

pub const Connection = struct {
    stream: net.Stream,
    recv_buf: RingBuffer,
    send_buf: std.ArrayList(u8),

    pub fn init(stream: net.Stream) !Connection {
        return .{
            .stream = stream,
            .recv_buf = try RingBuffer.init(4096),
            .send_buf = .empty,
        };
    }

    pub fn deinit(self: *Connection, alloc: std.mem.Allocator) void {
        self.recv_buf.deinit();
        self.send_buf.deinit(alloc);
        self.stream.close();
        self.* = undefined;
    }

    /// Read from `fd` into ring buffer. Returns bytes read, 0 means disconnect.
    pub fn recv(self: *Connection) !usize {
        const buf = self.recv_buf.writableSlice();
        if (buf.len == 0) return error.BufferFull;

        const n = posix.read(self.stream.handle, buf) catch |err| switch (err) {
            error.WouldBlock => return 0,
            else => return err,
        };
        if (n == 0) return 0; // disconnect

        self.recv_buf.commit(n);
        return n;
    }

    /// Flush `send_buf` to `fd`. Returns `true` if fully flushed.
    pub fn flush(self: *Connection) !bool {
        if (self.send_buf.items.len == 0) return true;
        const written = posix.write(self.stream.handle, self.send_buf.items) catch |err| switch (err) {
            error.WouldBlock => return false,
            else => return err,
        };
        std.mem.copyForwards(
            u8,
            self.send_buf.items[0 .. self.send_buf.items.len - written],
            self.send_buf.items[written..],
        );
        self.send_buf.shrinkRetainingCapacity(self.send_buf.items.len - written);
        return self.send_buf.items.len == 0;
    }

    pub fn queueSend(self: *Connection, alloc: std.mem.Allocator, data: []const u8) !void {
        try self.send_buf.appendSlice(alloc, data);
    }
};
