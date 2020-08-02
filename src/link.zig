const std = @import("std");
const warn = std.debug.warn;
const testing = std.testing;
const Tree = @import("tree.zig").Tree;
const KV = @import("kv.zig").KV;
const h = @import("hash.zig");
const Hash = h.Hash;

pub const LinkTag = enum {
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
      LinkTag.Pruned => undefined,
      LinkTag.Modified => self.Modified.tree.key(),
      LinkTag.Stored => self.Stored.tree.key(),
    };
  }

  pub fn tree(self: Link) ?*Tree {
    return switch(self) {
      LinkTag.Pruned => null,
      LinkTag.Modified => self.Modified.tree,
      LinkTag.Stored => self.Stored.tree,
    };
  }

  pub fn hash(self: Link) ?Hash {
    return switch(self) {
      LinkTag.Pruned => self.Pruned.hash,
      LinkTag.Modified => null,
      LinkTag.Stored => self.Stored.hash,
    };
  }

  pub fn height(self: Link) u8 {
    return switch(self) {
      LinkTag.Pruned => 1 + std.mem.max(u8, self.Pruned.child_heights[0..]),
      LinkTag.Modified => 1 + std.mem.max(u8, self.Modified.child_heights[0..]),
      LinkTag.Stored => 1 + std.mem.max(u8, self.Stored.child_heights[0..]),
    };
  }

  pub fn childHeights(self: Link) [2]u8 {
    return switch(self) {
      LinkTag.Pruned => self.Pruned.child_heights,
      LinkTag.Modified => self.Modified.child_heights,
      LinkTag.Stored => self.Stored.child_heights,
    };
  }

  pub fn balanceFactor(self: Link) i16 {
    return switch(self) {
      LinkTag.Pruned => @as(i16, self.Pruned.child_heights[1]) - @as(i16, self.Pruned.child_heights[0]),
      LinkTag.Modified => @as(i16, self.Modified.child_heights[1]) - @as(i16, self.Modified.child_heights[0]),
      LinkTag.Stored => @as(i16, self.Stored.child_heights[1]) - @as(i16, self.Stored.child_heights[0]),
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
      LinkTag.Pruned => Link{ .Stored = Stored{
        .hash = self.hash().?,
        .tree = t,
        .child_heights = t.childHeights()
      }},
      LinkTag.Modified => Link{ .Stored = Stored{
        .hash = self.tree().?.hash(),
        .tree = self.tree().?,
        .child_heights = self.childHeights(),
      }},
      LinkTag.Stored => @panic("should be modified link"),
    };
  }

  pub fn intoPruned(self: Link) Link {
    return switch(self) {
      LinkTag.Pruned => @panic("should be stored link"),
      LinkTag.Modified => @panic("should be stored link"),
      LinkTag.Stored => Link{ .Pruned = Pruned{ .hash = self.hash().?, .child_heights = self.childHeights() }},
    };
  }
};

pub const Pruned = struct {
  hash: Hash,
  child_heights: [2]u8, // [left, right]
  // key: []const u8,

  pub fn init(key: []const u8) Pruned {
    return Pruned{ .hash = undefined, .child_heights = .{0, 0}, .key = key };
  }
};

pub const Modified = struct {
  child_heights: [2]u8, // [left, right]
  tree: *Tree,
};

pub const Stored = struct {
  hash: Hash,
  child_heights: [2]u8, // [left, right]
  tree: *Tree,

  pub fn init(key: []const u8) Stored {
    return Stored{ .key = key };
  }
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
