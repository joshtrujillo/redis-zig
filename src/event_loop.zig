// src/event_loop.zig

const std = @import("std");
const posix = std.posix;

pub const Event = struct {
    fd: posix.socket_t,
    readable: bool,
    writable: bool,
    err: bool,
};

pub fn Reactor(comptime Backend: type) type {
    return struct {
        backend: Backend,

        const Self = @This();

        pub fn init(alloc: std.mem.Allocator) !Self {
            return .{ .backend = try Backend.init(alloc) };
        }

        pub fn deinit(self: *Self) void {
            self.backend.deinit();
        }

        pub fn register(self: *Self, fd: posix.socket_t) !void {
            try self.backend.register(fd);
        }

        pub fn unregister(self: *Self, fd: posix.socket_t) void {
            self.backend.unregister(fd);
        }

        pub fn wait(self: *Self, timeout_ms: i32) ![]const Event {
            return self.backend.wait(timeout_ms);
        }
    };
}

pub const PollBackend = struct {
    poll_fds: std.ArrayList(posix.pollfd),
    events: std.ArrayList(Event),
    alloc: std.mem.Allocator,

    pub fn init(alloc: std.mem.Allocator) !PollBackend {
        return .{
            .poll_fds = .empty,
            .events = .empty,
            .alloc = alloc,
        };
    }

    pub fn deinit(self: *PollBackend) void {
        self.poll_fds.deinit(self.alloc);
        self.events.deinit(self.alloc);
    }

    pub fn register(self: *PollBackend, fd: posix.socket_t) !void {
        try self.poll_fds.append(self.alloc, .{
            .fd = fd,
            .events = posix.POLL.IN,
            .revents = 0,
        });
    }

    pub fn unregister(self: *PollBackend, fd: posix.socket_t) void {
        for (self.poll_fds.items, 0..) |pfd, i| {
            if (pfd.fd == fd) {
                _ = self.poll_fds.swapRemove(i);
                return;
            }
        }
    }

    pub fn wait(self: *PollBackend, timeout_ms: i32) ![]const Event {
        _ = try posix.poll(self.poll_fds.items, timeout_ms);

        self.events.clearRetainingCapacity();
        for (self.poll_fds.items) |pfd| {
            if (pfd.revents == 0) continue;
            try self.events.append(self.alloc, .{
                .fd = pfd.fd,
                .readable = pfd.revents & posix.POLL.IN != 0,
                .writable = pfd.revents & posix.POLL.OUT != 0,
                .err = pfd.revents & posix.POLL.ERR != 0,
            });
        }
        return self.events.items;
    }
};
