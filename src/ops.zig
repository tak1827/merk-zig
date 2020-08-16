const std = @import("std");
const Allocator = std.mem.Allocator;
const testing = std.testing;
const Tree = @import("tree.zig").Tree;
const DB = @import("db.zig").RocksDataBbase;

// TODO: change these as config
pub const BatcSizeLimit = 10_000;
pub const BatchKeyLimit = 1024;
pub const BatchValueLimit = 1024;

pub const OpTag = enum(u1) { Put, Del };
pub const OpError = error{ DeleteNonexistantKey };

pub const Op = struct {
    const Self = @This();
    op: OpTag,
    key: []const u8,
    val: []const u8,
};

pub fn applyTo(allocator: *Allocator, db: *DB, tree: ?*Tree, batch: []Op) OpError!*Tree {
    if (tree) |t| return try apply(allocator, t, batch);
    return try build(allocator, db, batch);
}

pub fn build(allocator: *Allocator, db: *DB, batch: []Op) OpError!*Tree {
    var mid_index: usize = batch.len / 2;
    if (batch[mid_index].op == OpTag.Del) return error.DeleteNonexistantKey;
    var mid_tree = Tree.init(allocator, db, batch[mid_index].key, batch[mid_index].val) catch unreachable;
    return try recurse(allocator, mid_tree, batch, mid_index, true);
}

pub fn apply(allocator: *Allocator, tree: *Tree, batch: []Op) !*Tree {
    var found: bool = false;
    var mid: usize = 0;
    binaryBatchSearch(tree.key(), batch, &found, &mid);

    if (found) {
        if (batch[mid].op == OpTag.Put) {
            tree.updateVal(batch[mid].val);
        } else if (batch[mid].op == OpTag.Del) {
            // TODO: delete
        }
    }

    return try recurse(allocator, tree, batch, mid, found);
}

pub fn recurse(allocator: *Allocator, tree: *Tree, batch: []Op, mid: usize, exclusive: bool) OpError!*Tree {
    var left_batch = batch[0..mid];
    var right_batch = if (exclusive) batch[mid + 1 ..] else batch[mid..];

    if (left_batch.len != 0) {
        var detached = tree.detach(true);
        var applied = try applyTo(allocator, tree.db, detached, left_batch);
        tree.attach(true, applied);
    }

    if (right_batch.len != 0) {
        var detached = tree.detach(false);
        var applied = try applyTo(allocator, tree.db, detached, right_batch);
        tree.attach(false, applied);
    }

    return balance(tree);
}

pub fn balance(tree: *Tree) *Tree {
    var factor = balanceFactor(tree);
    if (-1 <= factor and factor <= 1) return tree;

    var is_left = factor < 0;
    var child_left = balanceFactor(tree.child(is_left)) > 0;

    if ((is_left and child_left) or (!is_left and !child_left)) {
        // Note: expected to have child
        var child = tree.detach(is_left).?;
        var rotated = rotate(child, !is_left);
        tree.attach(is_left, rotated);
    }

    return rotate(tree, is_left);
}

pub fn balanceFactor(tree: ?*Tree) i16 {
    if (tree) |t| return t.balanceFactor();
    return 0;
}

pub fn rotate(tree: *Tree, is_left: bool) *Tree {
    // Note: expected to have child
    var child = tree.detach(is_left).?;

    if (child.detach(!is_left)) |grand_child| tree.attach(is_left, grand_child);

    var balanced = balance(tree);
    child.attach(!is_left, balanced);

    var balanced_child = balance(child);
    return balanced_child;
}

pub fn binaryBatchSearch(needle: []const u8, batch: []Op, found: *bool, index: *usize) void {
    var low: usize = 0;
    var high: usize = batch.len - 1;
    while (low <= high) {
        const median = (low + high) / 2;
        if (std.mem.eql(u8, batch[median].key, needle)) {
            found.* = true;
            index.* = median;
            return;
        } else if (std.mem.lessThan(u8, batch[median].key, needle)) {
            low = median + 1;
        } else {
            if (median == 0) break;
            high = median - 1;
        }
    }

    found.* = false;
    index.* = low;
    return;
}

pub fn sortBatch(batch: []Op) void {
    std.sort.sort(Op, batch, {}, batchCmpLessThan);
}

fn batchCmpLessThan(context: void, a: Op, b: Op) bool {
    return std.mem.lessThan(u8, a.key, b.key);
}

test "apply" {
    // insert & update case
    var buf: [65536]u8 = undefined;
    var buffer = std.heap.FixedBufferAllocator.init(&buf);
    var arena = std.heap.ArenaAllocator.init(&buffer.allocator);
    defer arena.deinit();

    var op0 = Op{ .op = OpTag.Put, .key = "key0", .val = "value" };
    var op1 = Op{ .op = OpTag.Put, .key = "key1", .val = "value" };
    var op2 = Op{ .op = OpTag.Put, .key = "key2", .val = "value" };
    var op3 = Op{ .op = OpTag.Put, .key = "key3", .val = "value" };
    var op4 = Op{ .op = OpTag.Put, .key = "key4", .val = "value" };
    var op5 = Op{ .op = OpTag.Put, .key = "key5", .val = "value" };
    var op6 = Op{ .op = OpTag.Put, .key = "key6", .val = "value" };
    var op7 = Op{ .op = OpTag.Put, .key = "key7", .val = "value" };
    var op8 = Op{ .op = OpTag.Put, .key = "key8", .val = "value" };
    var op9 = Op{ .op = OpTag.Put, .key = "key9", .val = "value" };

    var batch1 = [_]Op{ op3, op6, op8 };
    var tree = try applyTo(arena.child_allocator, undefined, null, &batch1);
    testing.expect(tree.verify());
    testing.expectEqualSlices(u8, tree.key(), "key6");
    testing.expectEqualSlices(u8, tree.child(true).?.key(), "key3");
    testing.expectEqualSlices(u8, tree.child(false).?.key(), "key8");

    var batch2 = [_]Op{ op0, op1, op2, op3, op6, op8 };
    tree = try applyTo(arena.child_allocator, undefined, tree, &batch2);
    testing.expect(tree.verify());
    testing.expectEqualSlices(u8, tree.key(), "key3");
    testing.expectEqualSlices(u8, tree.child(true).?.key(), "key1");
    testing.expectEqualSlices(u8, tree.child(true).?.child(true).?.key(), "key0");
    testing.expectEqualSlices(u8, tree.child(true).?.child(false).?.key(), "key2");
    testing.expectEqualSlices(u8, tree.child(false).?.key(), "key6");
    testing.expectEqualSlices(u8, tree.child(false).?.child(false).?.key(), "key8");

    var batch3 = [_]Op{ op0, op4, op5, op7, op9 };
    tree = try applyTo(arena.child_allocator, undefined, tree, &batch3);
    testing.expect(tree.verify());
    testing.expectEqualSlices(u8, tree.key(), "key3");
    testing.expectEqualSlices(u8, tree.child(true).?.key(), "key1");
    testing.expectEqualSlices(u8, tree.child(true).?.child(true).?.key(), "key0");
    testing.expectEqualSlices(u8, tree.child(true).?.child(false).?.key(), "key2");
    testing.expectEqualSlices(u8, tree.child(false).?.key(), "key6");
    testing.expectEqualSlices(u8, tree.child(false).?.child(true).?.key(), "key5");
    testing.expectEqualSlices(u8, tree.child(false).?.child(true).?.child(true).?.key(), "key4");
    testing.expectEqualSlices(u8, tree.child(false).?.child(false).?.key(), "key8");
    testing.expectEqualSlices(u8, tree.child(false).?.child(false).?.child(true).?.key(), "key7");
    testing.expectEqualSlices(u8, tree.child(false).?.child(false).?.child(false).?.key(), "key9");

    // TODO: delete case
}

test "build" {
    var batch = [_]Op{
        Op{ .op = OpTag.Put, .key = "key2", .val = "value2" },
        Op{ .op = OpTag.Put, .key = "key3", .val = "value3" },
        Op{ .op = OpTag.Put, .key = "key5", .val = "value5" },
    };

    var tree = try build(testing.allocator, undefined, &batch);
    testing.expectEqualSlices(u8, tree.key(), "key3");
    testing.expectEqualSlices(u8, tree.child(true).?.key(), "key2");
    testing.expectEqualSlices(u8, tree.child(false).?.key(), "key5");
    defer testing.allocator.destroy(tree);
    defer testing.allocator.destroy(tree.child(true).?);
    defer testing.allocator.destroy(tree.child(false).?);
}

test "binaryBatchSearch" {
    var batch = [_]Op{
        Op{ .op = OpTag.Put, .key = "key2", .val = "value" },
        Op{ .op = OpTag.Put, .key = "key3", .val = "value" },
        Op{ .op = OpTag.Del, .key = "key5", .val = undefined },
    };

    var found: bool = false;
    var index: usize = 0;
    binaryBatchSearch("key3", &batch, &found, &index);
    testing.expect(found);
    testing.expectEqual(index, 1);
    binaryBatchSearch("key5", &batch, &found, &index);
    testing.expect(found);
    testing.expectEqual(index, 2);
    binaryBatchSearch("key4", &batch, &found, &index);
    testing.expect(!found);
    testing.expectEqual(index, 2);
    binaryBatchSearch("key1", &batch, &found, &index);
    testing.expect(!found);
    testing.expectEqual(index, 0);
    binaryBatchSearch("key6", &batch, &found, &index);
    testing.expect(!found);
    testing.expectEqual(index, 3);
}

test "sortBatch" {
    var batch = [_]Op{
        Op{ .op = OpTag.Put, .key = "key0", .val = "value" },
        Op{ .op = OpTag.Put, .key = "key9", .val = "value" },
        Op{ .op = OpTag.Put, .key = "key6", .val = "value" },
        Op{ .op = OpTag.Put, .key = "key8", .val = "value" },
        Op{ .op = OpTag.Put, .key = "key2", .val = "value" },
    };

    sortBatch(&batch);

    var i: usize = 0;
    while(i < batch.len - 1) : (i += 1) {
        testing.expect(batchCmpLessThan({}, batch[i], batch[i+1]));
    }
}
