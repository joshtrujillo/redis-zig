// src/main.zig

const std = @import("std");
const server_mod = @import("server.zig");
const Server = server_mod.Server;
const ServerConfig = server_mod.ServerConfig;

const Flag = enum {
    @"--port",
    @"--help",
    @"-h",

    fn parse(s: []const u8) ?Flag {
        return std.meta.stringToEnum(Flag, s);
    }
};

fn parseArgs() ?ServerConfig {
    var config: ServerConfig = .{};
    var args = std.process.args();
    _ = args.skip();

    while (args.next()) |arg| {
        const flag = Flag.parse(arg) orelse {
            std.log.err("Unknown flag: {s}", .{arg});
            return null;
        };

        switch (flag) {
            .@"--port" => {
                const val = args.next() orelse {
                    std.log.err("--port requires a value", .{});
                    return null;
                };
                config.port = std.fmt.parseInt(u16, val, 10) catch {
                    std.log.err("Invalid port: {s}", .{val});
                    return null;
                };
            },
            .@"--help", .@"-h" => {
                printUsage();
                return null;
            },
        }
    }

    return config;
}

fn printUsage() void {
    const message =
        \\Usage: codecrafters-redis [options]
        \\
        \\Options:
        \\  --port <port>  Set the listening port (default: 6379)
        \\  --help, -h     Show this message
        \\
    ;
    _ = std.posix.write(std.posix.STDOUT_FILENO, message) catch return;
}

pub fn main() !void {
    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    const config = parseArgs() orelse return;

    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();

    var server = try Server.init(alloc, config);
    defer server.deinit();

    while (true) {
        const poll_timeout_ms = server.computeTimeout();
        const events = try server.reactor.wait(poll_timeout_ms);
        server.expireBlockedClients();
        defer _ = arena.reset(.retain_capacity);

        for (events) |ev| {
            if (ev.fd == server.listener.stream.handle) {
                if (ev.readable) try server.acceptClient();
                continue;
            }

            if (ev.err) {
                server.removeClient(ev.fd);
                continue;
            }

            if (!ev.readable) continue;

            const client = server.clients.getPtr(ev.fd) orelse continue;

            const n = client.conn.recv() catch 0;
            if (n == 0) {
                std.log.info("Client disconnected - fd: {d}", .{ev.fd});
                server.removeClient(ev.fd);
                continue;
            }

            while (try client.parser.feed(arena.allocator(), client.conn.recv_buf.readableSlice())) |result| {
                std.log.info(
                    "Client fd: {d} sent command: {s}",
                    .{ ev.fd, result.value.array[0].bulk_string },
                );
                client.conn.recv_buf.advance(result.consumed);
                try server.dispatch(ev.fd, result.value, arena.allocator());
            }

            _ = try client.conn.flush();
        }
    }
}
