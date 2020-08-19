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

fn fromInitToDeint(allocator: *Allocator, ops: []Op, i: usize) !u128 {
    var merk = try Merk.init(allocator, "dbtest");
    if (i == 0) merk.tree = null; // init

    var timer = try time.Timer.start();

    try merk.apply(ops);
    try merk.commit();

    const runtime = timer.read();
    std.debug.print("counter: {}, runtime: {}\n", .{ i, runtime });

    merk.deinit();

    return runtime;
}

test "benchmark: add and put with no commit" {
    var batch_buf: [8_000_000]u8 = undefined;
    var batch_fixed_buf = heap.FixedBufferAllocator.init(&batch_buf);

    var merk_buf: [8_000_000]u8 = undefined;
    var merk_fixed_buf = heap.FixedBufferAllocator.init(&merk_buf);

    const batch_size: usize = 4_000;
    var ops: [batch_size]Op = undefined;

    var runtime_sum: u128 = 0;
    var i: usize = 0;
    var loop: usize = 10;
    while (i < loop) : (i += 1) {

        // prepare batch
        var batch_arena = heap.ArenaAllocator.init(&batch_fixed_buf.allocator);
        try buildBatch(&batch_arena.allocator, &ops, batch_size);
        o.sortBatch(&ops);
        var merk_arena = heap.ArenaAllocator.init(&merk_fixed_buf.allocator);

        runtime_sum += try fromInitToDeint(&merk_arena.allocator, &ops, i);
        doNotOptimize(fromInitToDeint);

        merk_arena.deinit();
        batch_arena.deinit();
    }

    const runtime_mean = runtime_sum / loop;
    std.debug.print("Iterations: {}, Mean(ns): {}\n", .{ loop, runtime_mean });
}

/// Pretend to use the value so the optimizer cant optimize it out.
fn doNotOptimize(val: anytype) void {
    const T = @TypeOf(val);
    var store: T = undefined;
    @ptrCast(*volatile T, &store).* = val;
}
