const std = @import("std");
const stdout = std.fs.File.stdout();
const net = std.net;
const posix = std.posix;
const protocol = @import("protocol.zig");
const storage = @import("storage.zig");

const Client = struct {
    conn: net.Server.Connection,
    buf: [4096]u8 = undefined,
    buf_len: usize = 0,

    pub fn init(conn: net.Server.Connection) Client {
        return .{
            .conn = conn,
            .buf_len = 0,
        };
    }

    pub fn deinit(self: *Client) void {
        self.conn.stream.close();
    }
};

pub fn main() !void {
    const server_alloc = std.heap.page_allocator;

    var store = storage.Store.init(server_alloc);
    defer store.deinit();

    // Server setup
    const address = try net.Address.resolveIp("127.0.0.1", 6379);
    var server = try address.listen(.{ .reuse_address = true });
    defer server.deinit();

    // Poll array setup
    const flags = try posix.fcntl(server.stream.handle, posix.F.GETFL, 0);
    _ = try posix.fcntl(server.stream.handle, posix.F.SETFL, flags | 0x800);

    var poll_fds: std.ArrayList(posix.pollfd) = .empty;
    defer poll_fds.deinit(server_alloc);

    try poll_fds.append(server_alloc, .{
        .fd = server.stream.handle,
        .events = posix.POLL.IN,
        .revents = 0,
    });

    var clients = std.AutoHashMap(posix.socket_t, Client).init(server_alloc);
    defer {
        var it = clients.valueIterator();
        while (it.next()) |c| c.deinit();
        clients.deinit();
    }

    while (true) {
        _ = try posix.poll(poll_fds.items, -1);

        // Check for new connections
        if (poll_fds.items[0].revents & posix.POLL.IN != 0) {
            const conn = try server.accept();

            try clients.put(conn.stream.handle, .{ .conn = conn });
            try poll_fds.append(server_alloc, .{
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
                const client = clients.getPtr(pfd.fd).?;

                // Arena allocator so we can just free everything at once
                var arena = std.heap.ArenaAllocator.init(server_alloc);
                defer arena.deinit();
                const conn_alloc = arena.allocator();

                // Read from client
                const available_space = client.buf[client.buf_len..];
                const n = posix.read(pfd.fd, available_space) catch 0;

                if (n == 0) {
                    // Client disconnected
                    std.log.info("Client disconnected: fd {d}", .{pfd.fd});
                    var c = clients.fetchRemove(pfd.fd).?;
                    c.value.deinit();
                    _ = poll_fds.swapRemove(i);
                    continue;
                }

                client.buf_len += n;

                const current_data = client.buf[0..client.buf_len];

                const result = protocol.parse(conn_alloc, current_data) catch |err| {
                    if (err == error.IncompleteCommand) { i += 1; continue; }
                    return err;
                };
                
                const response = try protocol.handleCommand(conn_alloc, &store, result.value);
                
                _ = try posix.write(pfd.fd, response);

                // Shift remaining data to the front of the buffer
                const remaining = client.buf_len - result.consumed;
                std.mem.copyForwards(u8, client.buf[0..remaining], client.buf[result.consumed..client.buf_len]);
                client.buf_len = remaining;
            }
            i += 1;
        }
    }
}
