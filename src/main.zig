const std = @import("std");
const stdout = std.fs.File.stdout();
const net = std.net;

pub fn main() !void {
    try stdout.writeAll("Logs from your program will appear here!");

    const address = try net.Address.resolveIp("127.0.0.1", 6379);

    var listener = try address.listen(.{
        .reuse_address = true,
    });
    defer listener.deinit();

    while (true) {
        const connection = try listener.accept();
        defer connection.stream.close();
        try stdout.writeAll("accepted new connection");

        // Reader
        var recv_buf: [1024]u8 = undefined;
        var stream_reader = connection.stream.reader(&recv_buf);
        const reader: *std.Io.Reader = stream_reader.interface();

        while (true) {
            reader.takeDelimiterExclusive('\n') catch |err| switch (err) {
                error.EndOfStream => break,
                else => return err,
            };
            try connection.stream.writeAll("+PONG\r\n");
        }
    }
}
