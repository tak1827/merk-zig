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

        var buf: [1024]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&buf);
        const top_key_len = try db.read(root_key, fbs.writer());
        if (top_key_len == 0) return merk;

        var tree = Tree.fetchTrees(allocator, &db, fbs.getWritten());
        merk.tree = tree;
        return merk;
    }

    pub fn deinit(self: Merk) void {
        // var db = @ptrCast(*DB, &self.db);
        self.db.deinit();
        // db.deinit();
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

        for (batch) |op| {
            if (std.mem.lessThan(u8, op.key, pre_key)) {
                std.debug.print("keys in batch must be sorted\n", .{});
                return error.InvalidOrder;
            } else if (std.mem.eql(u8, op.key, pre_key)) {
                std.debug.print("keys in batch must be unique, {}\n", .{op.key});
                return error.Invalid;
            }

            pre_key = op.key;
        }

        self.applyUnchecked(batch);
    }

    pub fn applyUnchecked(self: *Merk, batch: []Op) void {
        self.tree = o.applyTo(self.allocator, &self.db, self.tree, batch);
    }

    pub fn commit(self: *Merk) !void {
        var commiter: Commiter = undefined;

        if (self.tree) |tree| {
            commiter = try Commiter.init(self.allocator, &self.db, tree.height());
            tree.commit(&commiter);
            commiter.put(root_key, &self.rootHash().inner);
        } else {
            // TODO: delete root key
        }

        try commiter.commit();
    }
};

fn buildBatch(allocator: *Allocator, ops: []Op, loop: usize) !void {
    var i: usize = 0;
    var buffer: [100]u8 = undefined;
    while(i < loop) : (i += 1) {
        const key = try Hash.initPtr(allocator, U.intToString(&buffer, @as(u64, i)));
        var buf: [512]u8 = undefined;
        const val_slice = buf[0..U.randRepeatString(&buf, 98, 512, u9, @as(u64, i))];
        var val = try allocator.alloc(u8, val_slice.len);
        std.mem.copy(u8, val, val_slice);
        ops[i] = Op{ .op = OpTag.Put, .key = &key.inner, .val = val };
    }
}

// NOTE: error happen -> (error - detected leaked allocations without matching free: 2)
test "benchmark: add and put with no commit" {
    // var batch_buf: [10_000_000]u8 = undefined;
    // var batch_fixed_buf = heap.FixedBufferAllocator.init(&batch_buf);
    // var batch_arena = heap.ArenaAllocator.init(&batch_fixed_buf.allocator);
    // defer batch_arena.deinit();

    // const loop: usize = 10_000;
    // var ops: [loop]Op = undefined;
    // try buildBatch(&batch_arena.allocator, &ops, loop);

    // o.sortBatch(&ops);

    // var merk_buf: [7_000_000]u8 = undefined;
    // var merk_fixed_buf = heap.FixedBufferAllocator.init(&merk_buf);
    // var merk_arena = heap.ArenaAllocator.init(&merk_fixed_buf.allocator);
    // defer merk_arena.deinit();

    // var merk = try Merk.init(&merk_arena.allocator, "dbtest");
    // defer merk.deinit();

    // // initialize db
    // merk.db.destroy("dbtest");
    // merk.tree = null;

    // try merk.apply(&ops);
    // testing.expect(merk.tree.?.verify());
}

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

    // initialize db
    merk.db.destroy("dbtest");
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

    var top_key = merk.rootHash().inner;
    var tree = Tree.fetchTrees(merk.allocator, &merk.db, &top_key);
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
