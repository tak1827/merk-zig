const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const KV = @import("kv.zig").KV;
const Link = @import("link.zig").Link;
const LinkTag = @import("link.zig").LinkTag;
const Pruned = @import("link.zig").Pruned;
const Stored = @import("link.zig").Stored;
const h = @import("hash.zig");
const Hash = h.Hash;
const o = @import("ops.zig");
const Commiter = @import("commit.zig").Commiter;
const DB = @import("db.zig").DB;

// TODO: move to main
var tree_buf: [65536]u8 = undefined;
// TODO: switch to ArenaAllocator
var buffer = std.heap.FixedBufferAllocator.init(&tree_buf);

pub const Tree = struct {
  kv: KV,
  left: ?Link,
  right: ?Link,

  pub fn init(k: []const u8, v: []const u8) *Tree {
    var tree = buffer.allocator.create(Tree) catch |err| @panic("BUG: failed to create Tree");
    // std.debug.print("tree inited: {}\n", .{&tree});
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

  pub fn kvHash(self: Tree) Hash {
    return self.kv.hash;
  }

  pub fn link(self: Tree, is_left: bool) ?Link {
    if (is_left) {
      return self.left;
    } else {
      return self.right;
    }
  }

  pub fn child(self: Tree, is_left: bool) ?*Tree {
    if (self.link(is_left)) |l| {
      if (@as(LinkTag, l) == LinkTag.Pruned) {
        var _h = l.hash().?.inner;
        // TODO: no unreachable
        var _child = Tree.fetchTree(&_h);
        return _child;
      }
      return l.tree();
    } else {
      return null;
    }
  }

  pub fn childHash(self: Tree, is_left: bool) Hash {
    if (self.link(is_left)) |l| {
      return l.hash().?;
    } else {
      return h.ZeroHash;
    }
  }

  pub fn hash(self: Tree) Hash {
    return h.nodeHash(self.kvHash(), self.childHash(true), self.childHash(false));
  }

  pub fn childHeight(self: Tree, is_left: bool) u8 {
    if(self.link(is_left)) |l| {
      return l.height();
    } else {
      return 0;
    }
  }

  pub fn childHeights(self: Tree) [2]u8 {
    return [2]u8{ self.childHeight(true), self.childHeight(false) };
  }

  pub fn height(self: Tree) u8 {
    return 1 + std.mem.max(u8, self.childHeights()[0..]);
  }

  pub fn setLink(self: *Tree, is_left: bool, l: ?Link) void {
    if (is_left) {
      self.left = l;
    } else {
      self.right = l;
    }
  }

  pub fn balanceFactor(self: *Tree) i16 {
    return @as(i16, self.childHeight(false)) - @as(i16, self.childHeight(true));
  }

  pub fn attach(self: *Tree, is_left: bool, tree: ?*Tree) void {
    if (tree) |t| {
      if (mem.eql(u8, t.key(), self.key())) {
        @panic("BUG: tried to attach tree with same key");
      }

      if (self.link(is_left)) |l| {
        @panic("BUG: tried to attach to tree slot, but it is already some");
      }

      var slot: Link = Link.fromModifiedTree(t);

      self.setLink(is_left, slot);
    } else {
      return;
    }
  }

  pub fn detach(self: *Tree, is_left: bool) ?*Tree {
    if (self.link(is_left)) |slot| {
      self.setLink(is_left, null);

      if (@as(LinkTag, slot) == LinkTag.Pruned) {
        var _h = slot.hash().?.inner;
        var _child = Tree.fetchTree(&_h);
        return _child;
      }

      return slot.tree();
    } else {
      return null;
    }
  }

  pub fn updateVal(self: *Tree, val: []const u8) void {
    self.kv.val = val;
    self.kv.hash = h.kvHash(self.kv.key, val);
  }

  pub fn commit(self: *Tree, c: *Commiter) void {
    if(self.link(true)) |l| {
      if (@as(LinkTag, l) == LinkTag.Modified) {
        l.tree().?.commit(c);
        self.setLink(true, l.intoStored(undefined));
      }
    }

    if(self.link(false)) |l| {
      if (@as(LinkTag, l) == LinkTag.Modified) {
        l.tree().?.commit(c);
        self.setLink(false, l.intoStored(undefined));
      }
    }

    Commiter.write(self);

    // TODO: free tree buf allocation
    if (c.prune(self)) {
      if (self.link(true)) |l| {
        self.setLink(true, l.intoPruned());
      }
      if (self.link(false)) |l| {
        self.setLink(false, l.intoPruned());
      }
    }
  }

  pub fn fetchTree(k: []const u8) *Tree {
    var buf: [1024]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    var w = fbs.writer();
    _= DB.read(k, w) catch unreachable;
    var tree = Tree.unmarshal(fbs.getWritten()) catch unreachable;
    return tree;
  }

  pub fn fetchTrees(k: []const u8) *Tree {
    var self = Tree.fetchTree(k);

    if (self.link(true)) |l| {
      var _h = l.hash().?.inner;
      var t = Tree.fetchTrees(_h[0..]);
      self.setLink(true, l.intoStored(t));
    }

    if (self.link(false)) |l| {
      var _h = l.hash().?.inner;
      var t = Tree.fetchTrees(_h[0..]);
      self.setLink(false, l.intoStored(t));
    }

    return self;
  }

  pub fn marshal(self: *Tree, w: anytype) !void {
    @setRuntimeSafety(false);
    try w.writeIntBig(u32, @truncate(u32, self.key().len));
    try w.writeIntBig(u32, @truncate(u32, self.value().len));
    if (self.link(true)) |l| try w.writeByte(0x01) else try w.writeByte(0x00);
    if (self.link(false)) |l| try w.writeByte(0x01) else try w.writeByte(0x00);
    try w.writeAll(self.key());
    try w.writeAll(self.value());
    if (self.link(true)) |l| try w.writeAll(l.hash().?.inner[0..]);
    if (self.link(false)) |l| try w.writeAll(l.hash().?.inner[0..]);
  }

  pub fn unmarshal(buf: []const u8) !*Tree {
    @setRuntimeSafety(false);
    comptime var ptr = 0;
    if (ptr + 4 + 4 + 1 + 1 > buf.len) return error.EndOfFile;
    const key_len = std.mem.readIntBig(u32, std.mem.asBytes(buf[ptr .. ptr + 4]));
    ptr += 4;
    const val_len = std.mem.readIntBig(u32, std.mem.asBytes(buf[ptr .. ptr + 4]));
    ptr += 4;
    const left_flg = buf[ptr];
    ptr += 1;
    const right_flg = buf[ptr];
    ptr += 1;

    var total: usize = 4 + 4 + 1 + 1 + key_len + val_len;
    if (left_flg == 0x01) total += 32;
    if (right_flg == 0x01) total += 32;
    if (buf.len != total) return error.EndOfFile;

    const k = buf[ptr .. ptr + key_len];
    const v = buf[ptr + key_len .. ptr + key_len + val_len];
    var self = Tree.init(k, v);

    var hash_buf = buf[ptr + key_len + val_len .. ptr + key_len + val_len + h.HashLen];
    if ((left_flg == 0x01 and right_flg == 0x00)) {
      self.left = Link.fromMarshal(hash_buf);
    } else if ((left_flg == 0x00 and right_flg == 0x01)) {
      self.right = Link.fromMarshal(hash_buf);
    } else if ((left_flg == 0x01 and right_flg == 0x01)) {
      self.left = Link.fromMarshal(hash_buf);
      hash_buf = buf[ptr + key_len + val_len + h.HashLen .. ptr + key_len + val_len + h.HashLen + h.HashLen];
      self.right = Link.fromMarshal(hash_buf);
    }

    return self;
  }

  pub fn verify(self: *Tree) bool {
    if (self.link(true)) |l| {
      if (@as(LinkTag, l) != LinkTag.Pruned) {
        if (std.mem.lessThan(u8, self.key(), l.key())) @panic("unbalanced tree");
        _ = l.tree().?.verify();
      }
    }

    if (self.link(false)) |l| {
      if (@as(LinkTag, l) != LinkTag.Pruned) {
        if (!std.mem.lessThan(u8, self.key(), l.key())) @panic("unbalanced tree");
        _ = l.tree().?.verify();
      }
    }
    return true;
  }
};

test "marshal and unmarshal" {
  var hash_l = h.kvHash("leftkey", "leftvalue");
  var hash_r = h.kvHash("rightkey", "rightvalue");
  var left = Link{ .Stored = Stored{ .hash = hash_l, .child_heights = undefined, .tree = undefined} };
  var right = Link{ .Stored = Stored{ .hash = hash_r, .child_heights = undefined, .tree = undefined} };
  var tree: Tree = Tree{ .kv = KV.init("key", "value"), .left = left, .right = right };
  var buf: [255]u8 = undefined;
  var fbs = std.io.fixedBufferStream(&buf);
  var w = fbs.writer();
  try tree.marshal(w);
  var marshaled: []const u8 = fbs.getWritten();
  var unmarshaled = try Tree.unmarshal(marshaled);

  assert(mem.eql(u8, unmarshaled.key(), "key"));
  assert(mem.eql(u8, unmarshaled.value(), "value"));
  assert(mem.eql(u8, unmarshaled.link(true).?.hash().?.inner[0..], hash_l.inner[0..]));
  assert(mem.eql(u8, unmarshaled.link(false).?.hash().?.inner[0..], hash_r.inner[0..]));
}

test "detach" {
  var tree1 = Tree.init("key1", "value1");
  var tree2 = Tree.init("key2", "value2");
  tree1.attach(false, tree2);
  var tree3 = tree1.detach(false);
  assert(tree3 == tree2);
  assert(tree1.link(true) == null);
  assert(tree1.link(false) == null);
}

test "init" {
  const key = "key";
  const val = "value";
  const tree = Tree.init(key, val);
  assert(mem.eql(u8, tree.kv.key, key));
}

test "key" {
  var tree: Tree = Tree{ .kv = KV.init("key", "value"), .left = null, .right = null };
  assert(mem.eql(u8, tree.key(), "key"));
}

test "value" {
  var tree: Tree = Tree{ .kv = KV.init("key", "value"), .left = null, .right = null };
  assert(mem.eql(u8, tree.value(), "value"));
}

test "childHash" {
  const hash = h.kvHash("key", "value");
  var left: Link = Link{ .Pruned = Pruned{ .hash = hash, .child_heights = .{0, 0} } };
  var tree: Tree = Tree{ .kv = KV.init("key", "value"), .left = left, .right = null };
  assert(mem.eql(u8, tree.childHash(true).inner[0..], hash.inner[0..]));
  assert(mem.eql(u8, tree.childHash(false).inner[0..], h.ZeroHash.inner[0..]));
}

test "height" {
  var left: Link = Link{ .Pruned = Pruned{ .hash = undefined, .child_heights = .{0, 2} } };
  var tree: Tree = Tree{ .kv = undefined, .left = left, .right = null };
  assert(tree.height() == 4);
}

test "attach" {
  var tree1 = Tree.init("key1", "value1");
  var tree2 = Tree.init("key2", "value2");
  tree1.attach(false, tree2);
  assert(mem.eql(u8, tree1.right.?.tree().?.key()[0..], tree2.key()[0..]));
}
