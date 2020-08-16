const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const heap = std.heap;
const Tree = @import("tree.zig").Tree;
const o = @import("ops.zig");
const Op = o.Op;
const OpTag = o.OpTag;
const DB = @import("db.zig").RocksDataBbase;
const root_key = @import("db.zig").root_key;
const Hash = @import("hash.zig").HashBlake2s256;
const Commiter = @import("commit.zig").Commiter;
const LinkTag = @import("link.zig").LinkTag;
const U = @import("util.zig");

pub const Merk = struct {
    allocator: *Allocator,
    db: DB,
    tree: ?*Tree = null,

    pub fn init(allocator: *Allocator, name: ?[]const u8) !Merk {
        var db = try DB.init(name);
        var merk: Merk = Merk{ .allocator = allocator, .db = db, .tree = null };

        var buf: [o.BatchKeyLimit]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&buf);
        const top_key_len = try db.read(root_key, fbs.writer());
        if (top_key_len == 0) return merk;

        var tree = Tree.fetchTrees(allocator, &db, fbs.getWritten(), Commiter.DafaultLevels);
        merk.tree = tree;
        return merk;
    }

    pub fn deinit(self: Merk) void {
        self.db.deinit();
    }

    pub fn rootHash(self: *Merk) Hash {
        if (self.tree) |tree| {
            return tree.hash();
        } else {
            return Hash.zeroHash();
        }
    }

    pub fn apply(self: *Merk, batch: []Op) !void {
        var pre_key: []const u8 = "";

        if (batch.len > o.BatcSizeLimit) return error.ExceedBatchSizeLimit;

        for (batch) |op| {
            if (op.val.len > o.BatchValueLimit) return error.ExeceedBatchValueLimit;
            if (std.mem.lessThan(u8, op.key, pre_key)) {
                std.debug.print("keys in batch must be sorted\n", .{});
                return error.InvalidOrder;
            } else if (std.mem.eql(u8, op.key, pre_key)) {
                std.debug.print("keys in batch must be unique, {}\n", .{op.key});
                return error.Invalid;
            }

            pre_key = op.key;
        }

        try self.applyUnchecked(batch);
    }

    pub fn applyUnchecked(self: *Merk, batch: []Op) !void {
        self.tree = try o.applyTo(self.allocator, &self.db, self.tree, batch);
    }

    pub fn commit(self: *Merk) !void {
        var commiter: Commiter = undefined;

        if (self.tree) |tree| {
            commiter = try Commiter.init(self.allocator, &self.db, tree.height());
            tree.commit(&commiter);
            commiter.put(root_key, tree.key());
        } else {
            // TODO: delete root key
        }

        try commiter.commit();
    }

    pub fn get(self: *Merk, output: []u8, key: []const u8) usize {
        var tree = Tree.fetchTree(self.allocator, &self.db, key);
        self.allocator.destroy(tree);
        var val = tree.value();
        std.mem.copy(u8, output, val);
        return val.len;
    }
};

fn buildBatch(allocator: *Allocator, ops: []Op, comptime loop: usize) !void {
    var i: usize = 0;
    var buffer: [100]u8 = undefined;
    while(i < loop) : (i += 1) {
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

// test "benchmark: add and put with no commit" {
//     var batch_buf: [7_000_000]u8 = undefined;
//     var batch_fixed_buf = heap.FixedBufferAllocator.init(&batch_buf);

//     var merk_buf: [7_000_000]u8 = undefined;
//     var merk_fixed_buf = heap.FixedBufferAllocator.init(&merk_buf);

//     const loop: usize = 10_000;
//     var ops: [loop]Op = undefined;

//     var i: usize = 0;
//     while(i < 10) : (i += 1) {
//         std.debug.print("counter: {}\n", .{i});
//         var batch_arena = heap.ArenaAllocator.init(&batch_fixed_buf.allocator);
//         try buildBatch(&batch_arena.allocator, &ops, loop);
//         o.sortBatch(&ops);

//         var merk_arena = heap.ArenaAllocator.init(&merk_fixed_buf.allocator);
//         var merk = try Merk.init(&merk_arena.allocator, "dbtest");

//         // initialize
//         if (i == 0) merk.tree = null;

//         try merk.apply(&ops);
//         testing.expect(merk.tree.?.verify());

//         try merk.commit();
//         testing.expect(merk.tree.?.verify());

//         merk.deinit();
//         merk_arena.deinit();
//         batch_arena.deinit();
//     }
// }

test "init" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    var merk = try Merk.init(&arena.allocator, "dbtest");
    defer merk.deinit();
}

test "apply" {
    var merk: *Merk = undefined;

    var op0 = Op{ .op = OpTag.Put, .key = "key0", .val = "value" };
    var op1 = Op{ .op = OpTag.Put, .key = "key1", .val = "value" };
    var op2 = Op{ .op = OpTag.Put, .key = "key2", .val = "value" };

    var batch1 = [_]Op{ op0, op2, op1 };
    testing.expectError(error.InvalidOrder, merk.apply(&batch1));

    var batch2 = [_]Op{ op0, op2, op2 };
    testing.expectError(error.Invalid, merk.apply(&batch2));
}

test "apply and commit and fetch" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    var merk = try Merk.init(&arena.allocator, "dbtest");
    defer merk.deinit();
    defer merk.db.destroy("dbtest");

    merk.tree = null;

    // apply
    var op0 = Op{ .op = OpTag.Put, .key = "key0", .val = "value0" };
    var op1 = Op{ .op = OpTag.Put, .key = "key1", .val = "value1" };
    var op2 = Op{ .op = OpTag.Put, .key = "key2", .val = "value2" };
    var op3 = Op{ .op = OpTag.Put, .key = "key3", .val = "value3" };
    var op4 = Op{ .op = OpTag.Put, .key = "key4", .val = "value4" };
    var op5 = Op{ .op = OpTag.Put, .key = "key5", .val = "value5" };
    var op6 = Op{ .op = OpTag.Put, .key = "key6", .val = "value6" };
    var op7 = Op{ .op = OpTag.Put, .key = "key7", .val = "value7" };
    var op8 = Op{ .op = OpTag.Put, .key = "key8", .val = "value8" };
    var op9 = Op{ .op = OpTag.Put, .key = "key9", .val = "value9" };
    var batch = [_]Op{ op0, op1, op2, op3, op4, op5, op6, op7, op8, op9 };
    try merk.apply(&batch);

    testing.expectEqualSlices(u8, merk.tree.?.key(), "key5");
    testing.expectEqualSlices(u8, merk.tree.?.child(true).?.key(), "key2");
    testing.expectEqualSlices(u8, merk.tree.?.child(true).?.child(true).?.key(), "key1");
    testing.expectEqualSlices(u8, merk.tree.?.child(true).?.child(false).?.key(), "key4");
    testing.expectEqualSlices(u8, merk.tree.?.child(true).?.child(false).?.child(true).?.key(), "key3");
    testing.expectEqualSlices(u8, merk.tree.?.child(true).?.child(true).?.child(true).?.key(), "key0");
    testing.expectEqualSlices(u8, merk.tree.?.child(false).?.key(), "key8");
    testing.expectEqualSlices(u8, merk.tree.?.child(false).?.child(true).?.key(), "key7");
    testing.expectEqualSlices(u8, merk.tree.?.child(false).?.child(true).?.child(true).?.key(), "key6");
    testing.expectEqualSlices(u8, merk.tree.?.child(false).?.child(false).?.key(), "key9");

    // commit
    try merk.commit();
    testing.expect(merk.tree.?.verify());
    testing.expectEqual(@as(LinkTag, merk.tree.?.child(true).?.link(true).?), .Pruned);
    testing.expectEqual(@as(LinkTag, merk.tree.?.child(true).?.link(false).?), .Pruned);
    testing.expectEqual(@as(LinkTag, merk.tree.?.child(false).?.link(true).?), .Pruned);
    testing.expectEqual(@as(LinkTag, merk.tree.?.child(false).?.link(false).?), .Pruned);

    // fetch
    var top_key = merk.tree.?.key();
    var tree = Tree.fetchTrees(merk.allocator, &merk.db, top_key, Commiter.DafaultLevels);
    testing.expectEqualSlices(u8, merk.tree.?.key(), "key5");
    testing.expectEqualSlices(u8, merk.tree.?.child(true).?.key(), "key2");
    testing.expectEqualSlices(u8, merk.tree.?.child(true).?.value(), "value2");
    testing.expectEqualSlices(u8, merk.tree.?.child(true).?.child(true).?.key(), "key1");
    testing.expectEqualSlices(u8, merk.tree.?.child(true).?.child(false).?.key(), "key4");
    testing.expectEqualSlices(u8, merk.tree.?.child(true).?.child(false).?.child(true).?.key(), "key3");
    testing.expectEqualSlices(u8, merk.tree.?.child(true).?.child(false).?.child(true).?.value(), "value3");
    testing.expectEqualSlices(u8, merk.tree.?.child(true).?.child(true).?.child(true).?.key(), "key0");
    testing.expectEqualSlices(u8, merk.tree.?.child(false).?.key(), "key8");
    testing.expectEqualSlices(u8, merk.tree.?.child(false).?.child(true).?.key(), "key7");
    testing.expectEqualSlices(u8, merk.tree.?.child(false).?.child(true).?.child(true).?.key(), "key6");
    testing.expectEqualSlices(u8, merk.tree.?.child(false).?.child(false).?.key(), "key9");

    // get
    var output: [1024]u8 = undefined;
    var size = merk.get(&output, "key0");
    testing.expectEqualSlices(u8, output[0..size], "value0");
}

pub fn main() !void {
    var buf: [65536]u8 = undefined;
    var buffer = heap.FixedBufferAllocator.init(&buf);
    var arena = heap.ArenaAllocator.init(&buffer.allocator);
    defer arena.deinit();

    var merk = try Merk.init(&arena.allocator);
    defer merk.deinit();
    std.debug.print("merk.tree: {}\n", .{merk.tree});
}
