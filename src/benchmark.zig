const builtin = @import("builtin");
const std = @import("std");
const time = std.time;
const testing = std.testing;
const Allocator = std.mem.Allocator;
const heap = std.heap;
const Hash = @import("hash.zig").HashBlake2s256;
const o = @import("ops.zig");
const Op = o.Op;
const OpTag = o.OpTag;
const U = @import("util.zig");
const Merk = @import("merk.zig").Merk;

fn buildBatch(allocator: *Allocator, ops: []Op, comptime loop: usize) !void {
    var i: usize = 0;
    var buffer: [100]u8 = undefined;
    while (i < loop) : (i += 1) {
        // key
        const key = Hash.init(U.intToString(&buffer, @as(u64, i)));
        var key_buf = try std.ArrayList(u8).initCapacity(allocator, key.inner.len);
        defer key_buf.deinit();
        try key_buf.appendSlice(&key.inner);
        // val
        var buf: [255]u8 = undefined;
        const val = buf[0..U.randRepeatString(&buf, 98, 255, u8, @as(u64, i))];
        var val_buf = try std.ArrayList(u8).initCapacity(allocator, val.len);
        defer val_buf.deinit();
        try val_buf.appendSlice(val);

        ops[i] = Op{ .op = OpTag.Put, .key = key_buf.toOwnedSlice(), .val = val_buf.toOwnedSlice() };
    }
}

fn fromInitToDeint(allocator: *Allocator, ops: []Op, i: usize) !void {
    var merk = try Merk.init(allocator, "dbtest");

    // initialize
    if (i == 0) merk.tree = null;

    try merk.apply(ops);
    // testing.expect(merk.tree.?.verify());
    try merk.commit();
    // testing.expect(merk.tree.?.verify());

    merk.deinit();
}

test "benchmark: add and put with no commit" {
    var batch_buf: [2_000_000]u8 = undefined;
    var batch_fixed_buf = heap.FixedBufferAllocator.init(&batch_buf);

    var merk_buf: [5_000_000]u8 = undefined;
    var merk_fixed_buf = heap.FixedBufferAllocator.init(&merk_buf);

    const batch_size: usize = 1_000;
    var ops: [batch_size]Op = undefined;

    var timer = try time.Timer.start();
    var runtime_sum: u128 = 0;
    var i: usize = 0;
    var loop: usize = 10;
    while (i < loop) : (i += 1) {
        std.debug.print("counter: {}\n", .{i});

        // prepare batch
        var batch_arena = heap.ArenaAllocator.init(&batch_fixed_buf.allocator);
        try buildBatch(&batch_arena.allocator, &ops, batch_size);
        o.sortBatch(&ops);
        var merk_arena = heap.ArenaAllocator.init(&merk_fixed_buf.allocator);

        timer.reset();
        try fromInitToDeint(&merk_arena.allocator, &ops, i);
        const runtime = timer.read();
        runtime_sum += runtime;
        doNotOptimize(fromInitToDeint);

        merk_arena.deinit();
        batch_arena.deinit();
    }

    const runtime_mean = runtime_sum / i;
    std.debug.print("Iterations: {}, Mean(ns): {}\n", .{ loop, runtime_mean });
}

/// Pretend to use the value so the optimizer cant optimize it out.
fn doNotOptimize(val: anytype) void {
    const T = @TypeOf(val);
    var store: T = undefined;
    @ptrCast(*volatile T, &store).* = val;
}
