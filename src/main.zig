const std = @import("std");
const stdout = std.fs.File.stdout();
const net = std.net;
const posix = std.posix;

pub fn main() !void {
    const alloc = std.heap.page_allocator;
    try stdout.writeAll("Logs from your program will appear here!");

    const address = try net.Address.resolveIp("127.0.0.1", 6379);
    var server = try address.listen(.{
        .reuse_address = true,
    });
    defer server.deinit();

    const flags = try posix.fcntl(server.stream.handle, posix.F.GETFL, 0);
    try posix.fcntl(server.stream.handle, posix.F.SETFL, flags | posix.O.NONBLOCK);

    var poll_fds = std.ArrayList(posix.pollfd).init(alloc);
    defer poll_fds.deinit();

    try poll_fds.append(.{
        .fd = server.stream.handle,
        .events = posix.POLL.IN,
        .revents = 0,
    });

    while (true) {
        _ = try posix.poll(poll_fds.items, -1);

        if (poll_fds.items[0].revents & posix.POLL.IN != 0) {
            const connection = try server.accept();
            try poll_fds.append(.{
                .fd = connection.stream.handle,
                .events = posix.POLL.IN,
                .revents = 0,
            });
        }

        var i = 1;
        while (i < poll_fds.items.len) {
            const pfd = &poll_fds.items[i];
            
            if (pfd.revents & posix.POLL.IN != 0) {
                var buf: [1024]u8 = undefined;
                const n = posix.read(pfd.fd, &buf) catch 0;

                if (n == 0) {
                    // Client disconnected
                    posix.close(pfd.fd);
                    _ = poll_fds.swapRemove(i);
                    continue;
                } else {
                    _ = try posix.write(pfd.fd, "+PONG\r\n");
                }
                i += 1;
            }
            i += 1;
        }
    }
}

fn handle_connection(connection: net.Server.Connection) !void {
    try stdout.writeAll("accepted new connection");

    // Reader
    var recv_buf: [1024]u8 = undefined;
    var stream_reader = connection.stream.reader(&recv_buf);

    // Writer
    var send_buf: [1024]u8 = undefined;
    var stream_writer = connection.stream.writer(&send_buf);

    while (true) {
        while (stream_reader.interface().takeDelimiterInclusive('\n')) |line| {
            if (std.mem.startsWith(u8, line, "PING")) {
                _ = try stream_writer.interface.write("+PONG\r\n");
                _ = try stream_writer.interface.flush();
            }
        } else |_| {
            connection.stream.close();
        }
    }
}
