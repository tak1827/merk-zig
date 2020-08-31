const std = @import("std");
const testing = std.testing;
const Tree = @import("tree.zig").Tree;
const Merk = @import("merk.zig").Merk;

// TODO: change these as config
pub const BatcSizeLimit = 10_000;
pub const BatchKeyLimit = 1024;
pub const BatchValueLimit = 1024;

pub const OpTag = enum(u1) { Put, Del };
pub const OpError = error{DeleteNonexistantKey};

pub const Op = struct {
    const Self = @This();
    op: OpTag,
    key: []const u8,
    val: []const u8,
};

pub fn applyTo(tree: ?*Tree, batch: []Op) OpError!?*Tree {
    if (tree) |t| return try apply(t, batch);
    return try build(batch);
}

pub fn build(batch: []Op) OpError!*Tree {
    var mid_index: usize = batch.len / 2;
    if (batch[mid_index].op == OpTag.Del) return error.DeleteNonexistantKey;
    var mid_tree = Tree.init(batch[mid_index].key, batch[mid_index].val) catch unreachable;
    return try recurse(mid_tree, batch, mid_index, true);
}

pub fn apply(tree: *Tree, batch: []Op) !?*Tree {
    var found: bool = false;
    var mid: usize = 0;
    binaryBatchSearch(tree.key(), batch, &found, &mid);

    if (found) {
        if (batch[mid].op == OpTag.Put) {
            tree.updateVal(batch[mid].val);
        } else if (batch[mid].op == OpTag.Del) {
            var maybe_tree = remove(tree);

            var left_batch = batch[0..mid];
            var right_batch = batch[mid+1..];

            if (left_batch.len != 0) {
                maybe_tree = try applyTo(maybe_tree, left_batch);
            }

            if (right_batch.len != 0) {
                maybe_tree = try applyTo(maybe_tree, right_batch);
            }

            return maybe_tree;
        }
    }

    return try recurse(tree, batch, mid, found);
}

pub fn recurse(tree: *Tree, batch: []Op, mid: usize, exclusive: bool) OpError!*Tree {
    var left_batch = batch[0..mid];
    var right_batch = if (exclusive) batch[mid + 1 ..] else batch[mid..];

    if (left_batch.len != 0) {
        var detached = tree.detach(true);
        var applied = try applyTo(detached, left_batch);
        tree.attach(true, applied);
    }

    if (right_batch.len != 0) {
        var detached = tree.detach(false);
        var applied = try applyTo(detached, right_batch);
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

pub fn remove(tree: *Tree) ?*Tree {
    var has_left = if (tree.link(true)) |_| true else false;
    var has_right = if (tree.link(false)) |_| true else false;

    // no child
    if (!has_left and !has_right) return null;

    var is_left = tree.childHeight(true) > tree.childHeight(false);

    // single child
    if (!(has_left and has_right)) return tree.detach(is_left);

    // two child, promote edge of taller child
    var tall_child = tree.detach(is_left).?;
    var short_child = tree.detach(!is_left).?;
    return promoteEdge(tall_child, short_child, !is_left);
}

pub fn promoteEdge(tree: *Tree, attach: *Tree, is_left: bool) *Tree {
    var edge = removeEdge(tree, is_left);
    var _edge = edge.edge;

    _edge.attach(!is_left, edge.child);
    _edge.attach(is_left, attach);

    return balance(_edge);
}

const Edge = struct {
    edge: *Tree,
    child: ?*Tree,
};

pub fn removeEdge(tree: *Tree, is_left: bool) Edge {
    if (tree.link(is_left)) |_| {} else {
        return .{ .edge = tree, .child = tree.detach(!is_left) };
    }

    var child = tree.detach(is_left).?;
    var edge = removeEdge(child, is_left);

    tree.attach(is_left, edge.child);

    return .{ .edge = edge.edge, .child = balance(tree) };
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
    Merk.stack_allocator = &arena.allocator;
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
    var tree = try applyTo(null, &batch1);
    testing.expect(tree.?.verify());
    testing.expectEqualSlices(u8, tree.?.key(), "key6");
    testing.expectEqualSlices(u8, tree.?.child(true).?.key(), "key3");
    testing.expectEqualSlices(u8, tree.?.child(false).?.key(), "key8");

    var batch2 = [_]Op{ op0, op1, op2, op3, op6, op8 };
    tree = try applyTo(tree, &batch2);
    testing.expect(tree.?.verify());
    testing.expectEqualSlices(u8, tree.?.key(), "key3");
    testing.expectEqualSlices(u8, tree.?.child(true).?.key(), "key1");
    testing.expectEqualSlices(u8, tree.?.child(true).?.child(true).?.key(), "key0");
    testing.expectEqualSlices(u8, tree.?.child(true).?.child(false).?.key(), "key2");
    testing.expectEqualSlices(u8, tree.?.child(false).?.key(), "key6");
    testing.expectEqualSlices(u8, tree.?.child(false).?.child(false).?.key(), "key8");

    var batch3 = [_]Op{ op0, op4, op5, op7, op9 };
    tree = try applyTo(tree, &batch3);
    testing.expect(tree.?.verify());
    testing.expectEqualSlices(u8, tree.?.key(), "key3");
    testing.expectEqualSlices(u8, tree.?.child(true).?.key(), "key1");
    testing.expectEqualSlices(u8, tree.?.child(true).?.child(true).?.key(), "key0");
    testing.expectEqualSlices(u8, tree.?.child(true).?.child(false).?.key(), "key2");
    testing.expectEqualSlices(u8, tree.?.child(false).?.key(), "key6");
    testing.expectEqualSlices(u8, tree.?.child(false).?.child(true).?.key(), "key5");
    testing.expectEqualSlices(u8, tree.?.child(false).?.child(true).?.child(true).?.key(), "key4");
    testing.expectEqualSlices(u8, tree.?.child(false).?.child(false).?.key(), "key8");
    testing.expectEqualSlices(u8, tree.?.child(false).?.child(false).?.child(true).?.key(), "key7");
    testing.expectEqualSlices(u8, tree.?.child(false).?.child(false).?.child(false).?.key(), "key9");

    // TODO: delete case
    var op10 = Op{ .op = OpTag.Del, .key = "key0", .val = undefined };
    var op11 = Op{ .op = OpTag.Del, .key = "key1", .val = undefined };
    var op12 = Op{ .op = OpTag.Del, .key = "key2", .val = undefined };
    var op13 = Op{ .op = OpTag.Del, .key = "key3", .val = undefined };
    var op14 = Op{ .op = OpTag.Del, .key = "key4", .val = undefined };
    var op15 = Op{ .op = OpTag.Del, .key = "key5", .val = undefined };
    var op16 = Op{ .op = OpTag.Del, .key = "key6", .val = undefined };
    var op17 = Op{ .op = OpTag.Del, .key = "key7", .val = undefined };
    var op18 = Op{ .op = OpTag.Del, .key = "key8", .val = undefined };
    var op19 = Op{ .op = OpTag.Del, .key = "key9", .val = undefined };

    var batch4 = [_]Op{ op11, op15, op16, op19 };
    tree = try applyTo(tree, &batch4);
    testing.expectEqualSlices(u8, tree.?.key(), "key3");
    testing.expectEqualSlices(u8, tree.?.child(true).?.key(), "key2");
    testing.expectEqualSlices(u8, tree.?.child(true).?.child(true).?.key(), "key0");
    testing.expectEqualSlices(u8, tree.?.child(false).?.key(), "key7");
    testing.expectEqualSlices(u8, tree.?.child(false).?.child(true).?.key(), "key4");
    testing.expectEqualSlices(u8, tree.?.child(false).?.child(false).?.key(), "key8");

    var batch5 = [_]Op{ op12, op13, op17 };
    tree = try applyTo(tree, &batch5);
    testing.expect(tree.?.verify());

    var batch6 = [_]Op{ op10, op14, op18 };
    tree = try applyTo(tree, &batch6);
    testing.expect(tree == null);
}

test "build" {
    var batch = [_]Op{
        Op{ .op = OpTag.Put, .key = "key2", .val = "value2" },
        Op{ .op = OpTag.Put, .key = "key3", .val = "value3" },
        Op{ .op = OpTag.Put, .key = "key5", .val = "value5" },
    };
    Merk.stack_allocator = testing.allocator;

    var tree = try build(&batch);
    testing.expectEqualSlices(u8, tree.key(), "key3");
    testing.expectEqualSlices(u8, tree.child(true).?.key(), "key2");
    testing.expectEqualSlices(u8, tree.child(false).?.key(), "key5");

    Merk.stack_allocator.destroy(tree);
    Merk.stack_allocator.destroy(tree.child(true).?);
    Merk.stack_allocator.destroy(tree.child(false).?);
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
    while (i < batch.len - 1) : (i += 1) {
        testing.expect(batchCmpLessThan({}, batch[i], batch[i + 1]));
    }
}
