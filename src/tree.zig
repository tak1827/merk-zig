const std = @import("std");
const testing = std.testing;
const mem = std.mem;
const Allocator = std.mem.Allocator;
const KV = @import("kv.zig").KV;
const Link = @import("link.zig").Link;
const LinkTag = @import("link.zig").LinkTag;
const Pruned = @import("link.zig").Pruned;
const Stored = @import("link.zig").Stored;
const Hash = @import("hash.zig").HashBlake2s256;
const o = @import("ops.zig");
const Commiter = @import("commit.zig").Commiter;
const DB = @import("db.zig").RocksDataBbase;

pub const Tree = struct {
    allocator: *Allocator,
    db: *DB,
    kv: KV,
    left: ?Link,
    right: ?Link,

    pub fn init(allocator: *Allocator, db: *DB, k: []const u8, v: []const u8) !*Tree {
        var tree = try allocator.create(Tree);
        errdefer allocator.destroy(tree);

        tree.allocator = allocator;
        tree.db = db;
        tree.kv = KV.init(k, v);
        tree.left = null;
        tree.right = null;
        return tree;
    }

    pub fn key(self: Tree) []const u8 {
        return self.kv.key;
    }

    pub fn value(self: Tree) []const u8 {
        return self.kv.val;
    }

    pub fn updateVal(self: *Tree, val: []const u8) void {
        self.kv.hash = KV.kvHash(self.kv.key, self.kv.val);
    }

    pub fn hash(self: Tree) Hash {
        return KV.nodeHash(self.kvHash(), self.childHash(true), self.childHash(false));
    }

    pub fn childHash(self: Tree, is_left: bool) Hash {
        return if (self.link(is_left)) |l| l.hash().? else Hash.zeroHash();
    }

    pub fn kvHash(self: Tree) Hash {
        return self.kv.hash;
    }

    pub fn height(self: Tree) u8 {
        return 1 + mem.max(u8, self.childHeights()[0..]);
    }

    pub fn childHeights(self: Tree) [2]u8 {
        return [2]u8{ self.childHeight(true), self.childHeight(false) };
    }

    pub fn childHeight(self: Tree, is_left: bool) u8 {
        return if (self.link(is_left)) |l| l.height() else 0;
    }

    pub fn child(self: Tree, is_left: bool) ?*Tree {
        if (self.link(is_left)) |l| {
            if (@as(LinkTag, l) == .Pruned)
                return Tree.fetchTree(self.allocator, self.db, l.key());
            return l.tree();
        }
        return null;
    }

    pub fn link(self: Tree, is_left: bool) ?Link {
        return if (is_left) self.left else self.right;
    }

    pub fn setLink(self: *Tree, is_left: bool, l: ?Link) void {
        if (is_left) self.left = l else self.right = l;
    }

    pub fn balanceFactor(self: *Tree) i16 {
        return @as(i16, self.childHeight(false)) - @as(i16, self.childHeight(true));
    }

    pub fn attach(self: *Tree, is_left: bool, tree: ?*Tree) void {
        if (tree) |t| {
            if (mem.eql(u8, t.key(), self.key())) @panic("BUG: tried to attach tree with same key");
            if (self.link(is_left)) |l| @panic("BUG: tried to attach to tree slot, but it is already some");

            self.setLink(is_left, Link.fromModifiedTree(t));
        }
    }

    pub fn detach(self: *Tree, is_left: bool) ?*Tree {
        if (self.link(is_left)) |slot| {
            self.setLink(is_left, null);

            if (@as(LinkTag, slot) == .Pruned) {
                var _child = Tree.fetchTree(self.allocator, self.db, slot.key());
                return _child;
            }

            return slot.tree();
        }
        return null;
    }

    pub fn commit(self: *Tree, c: *Commiter) void {
        if (self.link(true)) |l| {
            if (@as(LinkTag, l) == .Modified) {
                l.tree().?.commit(c);
                self.setLink(true, l.intoStored(undefined));
            }
        }

        if (self.link(false)) |l| {
            if (@as(LinkTag, l) == .Modified) {
                l.tree().?.commit(c);
                self.setLink(false, l.intoStored(undefined));
            }
        }

        c.write(self);

        if (c.prune(self)) {
            if (self.link(true)) |l| {
                defer self.allocator.destroy(l.tree());
                self.setLink(true, l.intoPruned());
            }
            if (self.link(false)) |l| {
                defer self.allocator.destroy(l.tree());
                self.setLink(false, l.intoPruned());
            }
        }
    }

    pub fn fetchTree(allocator: *Allocator, db: *DB, k: []const u8) *Tree {
        const alloc = std.heap.page_allocator;
        var buf = std.ArrayList(u8).init(alloc);
        _ = db.read(k, buf.writer()) catch unreachable;
        defer buf.deinit();

        return Tree.unmarshal(allocator, db, buf.toOwnedSlice()) catch unreachable;
    }

    pub fn fetchTrees(allocator: *Allocator, db: *DB, k: []const u8, level: u8) *Tree {
        const self = Tree.fetchTree(allocator, db, k);

        if (level > 0) {
            if (self.link(true)) |l| {
                var t = Tree.fetchTrees(allocator, db, l.key(), level - 1);
                self.setLink(true, l.intoStored(t));
            }

            if (self.link(false)) |l| {
                var t = Tree.fetchTrees(allocator, db, l.key(), level - 1);
                self.setLink(false, l.intoStored(t));
            }
        }

        return self;
    }

    pub fn marshal(self: *Tree, w: anytype) !void {
        @setRuntimeSafety(false);
        try w.writeIntBig(u32, @truncate(u32, self.key().len));
        try w.writeAll(self.key());
        try w.writeIntBig(u32, @truncate(u32, self.value().len));
        try w.writeAll(self.value());
        if (self.link(true)) |l| {
            try w.writeByte(0x01);
            try w.writeAll(&l.hash().?.inner);
            try w.writeIntBig(u32, @truncate(u32, l.key().len));
            try w.writeAll(l.key());
        } else {
            try w.writeByte(0x00);
        }
        if (self.link(false)) |l| {
            try w.writeByte(0x01);
            try w.writeAll(&l.hash().?.inner);
            try w.writeIntBig(u32, @truncate(u32, l.key().len));
            try w.writeAll(l.key());
        } else {
            try w.writeByte(0x00);
        }
    }

    pub fn unmarshal(allocator: *Allocator, db: *DB, buf: []const u8) !*Tree {
        @setRuntimeSafety(false);
        var ptr: usize = 0;
        var bytes: [4]u8 = undefined;
        const hash_len = Hash.len();

        if (ptr + 4 + 4 + 1 + 1 > buf.len) return error.EndOfFile;

        // key
        mem.copy(u8, &bytes, buf[ptr..ptr+4]);
        ptr += 4;
        const key_len = mem.readIntBig(u32, &bytes);
        const k = buf[ptr..ptr+key_len];
        ptr += key_len;
        // val
        mem.copy(u8, &bytes, buf[ptr..ptr+4]);
        ptr += 4;
        const val_len = mem.readIntBig(u32, &bytes);
        const v = buf[ptr..ptr+val_len];
        ptr += val_len;

        var self = try Tree.init(allocator, db, k, v);

        // left
        const left_flg = buf[ptr];
        ptr += 1;
        if (left_flg == 0x01) {
            const hash_buf = buf[ptr..ptr+hash_len];
            ptr += hash_len;
            mem.copy(u8, &bytes, buf[ptr..ptr+4]);
            ptr += 4;
            const val_left_len = mem.readIntBig(u32, &bytes);
            const val_left = buf[ptr..ptr+val_left_len];
            ptr += val_left_len;
            self.left = Link.fromMarshal(hash_buf, val_left);
        }
        // right
        const right_flg = buf[ptr];
        ptr += 1;
        if (right_flg == 0x01) {
            const hash_buf = buf[ptr..ptr+hash_len];
            ptr += hash_len;
            mem.copy(u8, &bytes, buf[ptr..ptr+4]);
            ptr += 4;
            const val_right_len = mem.readIntBig(u32, &bytes);
            const val_right = buf[ptr..ptr+val_right_len];
            ptr += val_right_len;
            self.right = Link.fromMarshal(hash_buf, val_right);
        }

        return self;
    }

    pub fn verify(self: *Tree) bool {
        if (self.link(true)) |l| {
            if (@as(LinkTag, l) != .Pruned) {
                if (mem.lessThan(u8, self.key(), l.key())) @panic("unbalanced tree");
                _ = l.tree().?.verify();
            }
        }
        if (self.link(false)) |l| {
            if (@as(LinkTag, l) != .Pruned) {
                if (!mem.lessThan(u8, self.key(), l.key())) @panic("unbalanced tree");
                _ = l.tree().?.verify();
            }
        }
        return true;
    }
};

test "marshal and unmarshal" {
    // marshal
    var hash_l = KV.kvHash("leftkey", "leftvalue");
    var hash_r = KV.kvHash("rightkey", "rightvalue");
    var tree_l = Tree{ .allocator = undefined, .db = undefined, .kv = KV.init("keylefttree", "value"), .left = undefined, .right = undefined };
    var tree_r = Tree{ .allocator = undefined, .db = undefined, .kv = KV.init("keyrighttree", "value"), .left = undefined, .right = undefined };
    var left = Link{ .Stored = Stored{ .hash = hash_l, .child_heights = undefined, .tree = &tree_l } };
    var right = Link{ .Stored = Stored{ .hash = hash_r, .child_heights = undefined, .tree = &tree_r } };
    var tree: Tree = Tree{ .allocator = undefined, .db = undefined, .kv = KV.init("key", "value"), .left = left, .right = right };
    var buf: [255]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    try tree.marshal(fbs.writer());
    var marshaled: []const u8 = fbs.getWritten();

    // unmarshal
    var unmarshaled = try Tree.unmarshal(testing.allocator, undefined, marshaled);
    defer std.testing.allocator.destroy(unmarshaled);

    testing.expectEqualSlices(u8, unmarshaled.key(), "key");
    testing.expectEqualSlices(u8, unmarshaled.value(), "value");
    testing.expectEqualSlices(u8, unmarshaled.link(true).?.hash().?.inner[0..], &hash_l.inner);
    testing.expectEqualSlices(u8, unmarshaled.link(true).?.key(), tree_l.key());
    testing.expectEqualSlices(u8, unmarshaled.link(false).?.hash().?.inner[0..], hash_r.inner[0..]);
    testing.expectEqualSlices(u8, unmarshaled.link(false).?.key(), tree_r.key());

}

test "detach" {
    var tree1 = try Tree.init(testing.allocator, undefined, "key1", "value1");
    defer testing.allocator.destroy(tree1);
    var tree2 = try Tree.init(testing.allocator, undefined, "key2", "value2");
    defer testing.allocator.destroy(tree2);
    tree1.attach(false, tree2);
    var tree3 = tree1.detach(false);
    testing.expectEqual(tree3, tree2);
    testing.expectEqual(tree1.link(true), null);
    testing.expectEqual(tree1.link(false), null);
}

test "init" {
    const key = "key";
    const val = "value";
    const tree = try Tree.init(testing.allocator, undefined, key, val);
    defer testing.allocator.destroy(tree);

    testing.expectEqualSlices(u8, tree.kv.key, key);
}

test "key" {
    testing.expectEqualSlices(u8, Tree.key(Tree{ .allocator = undefined, .db = undefined, .kv = KV.init("key", "value"), .left = null, .right = null }), "key");
}

test "value" {
    testing.expectEqualSlices(u8, Tree.value(Tree{ .allocator = undefined, .db = undefined, .kv = KV.init("key", "value"), .left = null, .right = null }), "value");
}

test "childHash" {
    var hash = KV.kvHash("key", "value");
    var left: Link = Link{ .Pruned = Pruned{ .hash = hash, .child_heights = .{ 0, 0 }, .key = undefined } };
    var tree: Tree = Tree{ .allocator = undefined, .db = undefined, .kv = KV.init("key", "value"), .left = left, .right = null };
    testing.expectEqualSlices(u8, &tree.childHash(true).inner, &hash.inner);
    testing.expectEqualSlices(u8, &tree.childHash(false).inner, &Hash.zeroHash().inner);
}

test "height" {
    var left: Link = Link{ .Pruned = Pruned{ .hash = undefined, .child_heights = .{ 0, 2 }, .key = undefined } };
    testing.expectEqual(Tree.height(Tree{ .allocator = undefined, .db = undefined, .kv = undefined, .left = left, .right = null }), 4);
}

test "attach" {
    var tree1 = try Tree.init(testing.allocator, undefined, "key1", "value1");
    defer testing.allocator.destroy(tree1);
    var tree2 = try Tree.init(testing.allocator, undefined, "key2", "value2");
    defer testing.allocator.destroy(tree2);

    tree1.attach(false, tree2);
    testing.expectEqualSlices(u8, tree1.right.?.tree().?.key()[0..], tree2.key()[0..]);
}

pub fn main() !void {
    var buf: [65536]u8 = undefined;
    var buffer = std.heap.FixedBufferAllocator.init(&buf);
    var arena = std.heap.ArenaAllocator.init(&buffer.allocator);
    defer arena.deinit();

    var tree = try Tree.init(arena.child_allocator, "key", "value");
    std.debug.print("tree: {}\n", .{tree});
}
