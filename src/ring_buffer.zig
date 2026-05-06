// src/ring_buffer.zig

const std = @import("std");
const posix = std.posix;

pub const RingBuffer = struct {
    base: [*]u8,
    capacity: usize,
    read_pos: usize = 0,
    write_pos: usize = 0,
    fd: posix.fd_t,

    pub fn init(min_size: usize) !RingBuffer {
        const ps = std.heap.pageSize();
        const capacity = std.math.ceilPowerOfTwo(usize, @max(ps, min_size)) catch
            return error.Overflow;

        // Anonymous backing "file"
        const fd = try posix.memfd_createZ("ring", 0);
        errdefer posix.close(fd);
        try posix.ftruncate(fd, @intCast(capacity));

        // Reserve 2*capacity of contiguous virtual address space
        const reserved = try posix.mmap(
            null,
            2 * capacity,
            posix.PROT.NONE,
            .{ .TYPE = .PRIVATE, .ANONYMOUS = true },
            -1,
            0,
        );
        const base: [*]u8 = @ptrCast(reserved.ptr);
        errdefer posix.munmap(reserved);

        // Map the fd into the first half
        _ = try posix.mmap(
            @alignCast(base),
            capacity,
            posix.PROT.READ | posix.PROT.WRITE,
            .{ .TYPE = .SHARED, .FIXED = true },
            fd,
            0,
        );

        // Map the same fd at offset 0 into the second half
        _ = try posix.mmap(
            @alignCast(base + capacity),
            capacity,
            posix.PROT.READ | posix.PROT.WRITE,
            .{ .TYPE = .SHARED, .FIXED = true },
            fd,
            0,
        );

        return .{ .base = base, .capacity = capacity, .fd = fd };
    }

    pub fn deinit(self: *RingBuffer) void {
        const aligned: [*]align(std.heap.pageSize()) u8 = @alignCast(self.base);
        posix.munmap(aligned[0 .. 2 * self.capacity]);
        posix.close(self.fd);
        self.* = undefined;
    }

    // Read

    /// Contiguous view of all unread bytes.
    pub fn readableSlice(self: *const RingBuffer) []const u8 {
        const offset = self.read_pos & (self.capacity - 1);
        return self.base[offset .. offset + self.readable()];
    }

    pub fn readable(self: *const RingBuffer) usize {
        return self.write_pos - self.read_pos;
    }

    /// Consume `n` bytes from the read side.
    pub fn advance(self: *RingBuffer, n: usize) void {
        std.debug.assert(n <= self.readable());
        self.read_pos += n;
    }

    // Write

    /// Contiguous slice of available write space.
    /// Write into this, then call commit(n).
    pub fn writableSlice(self: *RingBuffer) []u8 {
        const offset = self.write_pos & (self.capacity - 1);
        return self.base[offset .. offset + self.writable()];
    }

    pub fn writable(self: *const RingBuffer) usize {
        return self.capacity - self.readable();
    }

    /// Mark n bytes as freshly written.
    pub fn commit(self: *RingBuffer, n: usize) void {
        std.debug.assert(n <= self.writable());
        self.write_pos += n;
    }
};

test "mirroring: write across boundary, read contiguously" {
    var rb = try RingBuffer.init(std.heap.pageSize());
    defer rb.deinit();

    // Fill most of the buffer
    const fill_size = rb.capacity - 4;
    @memset(rb.writableSlice()[0..fill_size], 'x');
    rb.commit(fill_size);

    // Consume it, moving read_pos near the end
    rb.advance(fill_size);

    // Now write 8 bytes that straddle the boundary:
    // 4 bytes before the end, 4 bytes wrapping to the start
    const msg = "ABCDEFGH";
    @memcpy(rb.writableSlice()[0..msg.len], msg);
    rb.commit(msg.len);

    // The magic: readableSlice() returns all 8 bytes contiguously
    // despite them straddling the physical boundary
    const slice = rb.readableSlice();
    try std.testing.expectEqualStrings("ABCDEFGH", slice);
}
