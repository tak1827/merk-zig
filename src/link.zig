const std = @import("std");
const warn = std.debug.warn;
const testing = std.testing;
const Tree = @import("tree.zig").Tree;
const KV = @import("kv.zig").KV;
const h = @import("hash.zig");
const Hash = h.Hash;

pub const LinkTag = enum(u2) {
    Pruned,
    Modified,
    Stored,
};

pub const Link = union(LinkTag) {
  Pruned: Pruned,
  Modified: Modified,
  Stored: Stored,

  pub fn key(self: Link) []const u8 {
    return switch(self) {
      .Pruned => undefined,
      .Modified => self.Modified.tree.key(),
      .Stored => self.Stored.tree.key(),
    };
  }

  pub fn tree(self: Link) ?*Tree {
    return switch(self) {
      .Pruned => null,
      .Modified => self.Modified.tree,
      .Stored => self.Stored.tree,
    };
  }

  pub fn hash(self: Link) ?Hash {
    return switch(self) {
      .Pruned => self.Pruned.hash,
      .Modified => null,
      .Stored => self.Stored.hash,
    };
  }

  pub fn height(self: Link) u8 {
    return switch(self) {
      .Pruned => 1 + std.mem.max(u8, self.Pruned.child_heights[0..]),
      .Modified => 1 + std.mem.max(u8, self.Modified.child_heights[0..]),
      .Stored => 1 + std.mem.max(u8, self.Stored.child_heights[0..]),
    };
  }

  pub fn childHeights(self: Link) [2]u8 {
    return switch(self) {
      .Pruned => self.Pruned.child_heights,
      .Modified => self.Modified.child_heights,
      .Stored => self.Stored.child_heights,
    };
  }

  pub fn balanceFactor(self: Link) i16 {
    return switch(self) {
      .Pruned => @as(i16, self.Pruned.child_heights[1]) - @as(i16, self.Pruned.child_heights[0]),
      .Modified => @as(i16, self.Modified.child_heights[1]) - @as(i16, self.Modified.child_heights[0]),
      .Stored => @as(i16, self.Stored.child_heights[1]) - @as(i16, self.Stored.child_heights[0]),
    };
  }

  pub fn fromModifiedTree(t: *Tree) Link {
    return Link{ .Modified = Modified{ .child_heights = t.childHeights(), .tree = t }};
  }

  pub fn fromMarshal(buf: []const u8) Link {
    var _h: Hash = undefined;
    std.mem.copy(u8, _h.inner[0..], buf);
    return Link{ .Pruned = Pruned{ .hash = _h, .child_heights = [2]u8{0, 0} }};
  }

  pub fn intoStored(self: Link, t: *Tree) Link {
    return switch(self) {
      .Pruned => Link{ .Stored = Stored{
        .hash = self.hash().?,
        .tree = t,
        .child_heights = t.childHeights()
      }},
      .Modified => Link{ .Stored = Stored{
        .hash = self.tree().?.hash(),
        .tree = self.tree().?,
        .child_heights = self.childHeights(),
      }},
      .Stored => @panic("should be modified link"),
    };
  }

  pub fn intoPruned(self: Link) Link {
    return switch(self) {
      .Pruned => @panic("should be stored link"),
      .Modified => @panic("should be stored link"),
      .Stored => Link{ .Pruned = Pruned{ .hash = self.hash().?, .child_heights = self.childHeights() }},
    };
  }
};

pub const Pruned = struct {
  hash: Hash,
  child_heights: [2]u8, // [left, right]
  // key: []const u8,
};

pub const Modified = struct {
  child_heights: [2]u8, // [left, right]
  tree: *Tree,
};

pub const Stored = struct {
  hash: Hash,
  child_heights: [2]u8, // [left, right]
  tree: *Tree,
};

test "key" {
  var tree: Tree = Tree{ .kv = KV.init("key", "value"), .left = null, .right = null };
  const l: Link = Link{ .Modified =  Modified{ .child_heights = .{0, 2},  .tree = &tree } };
  testing.expectEqualSlices(u8, l.key(), "key");
}

test "tree" {
  var tree: Tree = Tree{ .kv = KV.init("key", "value"), .left = null, .right = null };
  const l: Link = Link{ .Modified =  Modified{ .child_heights = .{0, 2},  .tree = &tree } };
  var linkedTree = l.tree().?;
  testing.expectEqual(&tree, linkedTree);
}

test "hash" {
  const kvHash = h.kvHash("key", "value");
  const l: Link = Link{ .Pruned =  Pruned{ .hash = kvHash, .child_heights = .{0, 2} } };
  testing.expectEqualSlices(u8, l.hash().?.inner[0..], kvHash.inner[0..]);
}

test "height" {
  const l: Link = Link{ .Modified =  Modified{ .child_heights = .{0, 2}, .tree = undefined } };
  testing.expectEqual(l.height(), 3);
}

test "balanceFactor" {
  const l: Link = Link{ .Modified =  Modified{ .child_heights = .{2, 0}, .tree = undefined } };
  testing.expectEqual(l.balanceFactor(), -2);
}
