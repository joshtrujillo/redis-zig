const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.graph.host;

    // ── Main executable ──
    const exe = b.addExecutable(.{
        .name = "main",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
        }),
    });
    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    if (b.args) |args| run_cmd.addArgs(args);
    const run_step = b.step("run", "Run the app");
    run_step.dependOn(&run_cmd.step);

    // ── Tests ──
    // Add every source file that contains tests.
    // `zig build test` runs them all; `zig build test -- --filter "XREAD"` filters.
    const test_files: []const []const u8 = &.{
        "src/protocol.zig",
        "src/storage.zig",
        "src/engine.zig",
    };

    const test_step = b.step("test", "Run all unit tests");

    for (test_files) |file| {
        const t = b.addTest(.{
            .root_module = b.createModule(.{
                .root_source_file = b.path(file),
                .target = target,
            }),
        });
        const run_t = b.addRunArtifact(t);
        test_step.dependOn(&run_t.step);
    }
}
