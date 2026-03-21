const std = @import("std");
const stdout = std.fs.File.stdout();
const net = std.net;
const posix = std.posix;
const protocol = @import("protocol.zig");

const Client = struct {
    conn: net.Server.Connection,
    
    pub fn deinit(self: *Client) void {
        self.conn.steam.close();
    }
};

pub fn main() !void {
    const alloc = std.heap.page_allocator;

    // Server setup
    const address = try net.Address.resolveIp("127.0.0.1", 6379);
    var server = try address.listen(.{ .reuse_address = true });
    defer server.deinit();

    // Poll array setup
    const flags = try posix.fcntl(server.stream.handle, posix.F.GETFL, 0);
    _ = try posix.fcntl(server.stream.handle, posix.F.SETFL, flags | 0x800);

    var poll_fds: std.ArrayList(posix.pollfd) = .empty;
    defer poll_fds.deinit(alloc);

    try poll_fds.append(alloc, .{
        .fd = server.stream.handle,
        .events = posix.POLL.IN,
        .revents = 0,
    });

    var clients = std.AutoHashMap(posix.socket_t, Client).init(alloc);
    defer {
        var it = clients.valueIterator();
        while (it.next()) |c| c.deinit();
        clients.deinit();
    }

    while (true) {
        try stdout.writeAll("Looping");
        _ = try posix.poll(poll_fds.items, -1);

        // Check for new connections
        if (poll_fds.items[0].revents & posix.POLL.IN != 0) {
            const conn = try server.accept();
            errdefer conn.stream.close();

            try stdout.writeAll("Accepted connection!");
            try clients.put(conn.stream.handle, .{ .conn = conn });
            try poll_fds.append(alloc, .{
                .fd = conn.stream.handle,
                .events = posix.POLL.IN,
                .revents = 0,
            });
            std.log.info("Accepted connection: fd {d}", .{conn.stream.handle});
        }

        // Check Clients
        var i: usize = 1;
        while (i < poll_fds.items.len) {
            const pfd = &poll_fds.items[i];
            if (pfd.revents & posix.POLL.IN != 0) {
                var buf: [1024]u8 = undefined;
                // const client = clients.getPtr(pfd.fd).?;

                // Read from client
                const n = posix.read(pfd.fd, &buf) catch 0;

                if (n == 0) {
                    // Client disconnected
                    std.log.info("Client disconnected: fd {d}", .{pfd.fd});
                    var c = clients.fetchRemove(pfd.fd).?;
                    c.value.deinit();
                    _ = poll_fds.swapRemove(i);
                    continue;
                }
                
                const response = protocol.handleCommand(buf[0..n]);
                _ = try posix.write(pfd.fd, "+PONG\r\n");
            }
            i += 1;
        }
    }
}
