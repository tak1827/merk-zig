const std = @import("std");
const c = @cImport(@cInclude("rocksdb/c.h"));
const testing = std.testing;
const Tree = @import("tree.zig").Tree;
const o = @import("ops.zig");
const Op = o.Op;
const OpTag = o.OpTag;
const db = @import("db.zig");
const DB = db.DB;

pub const Commiter = struct {
  height: u8,
  levels: u8,

  const DafaultLevels: u8 = 1;

  // TODO: pass level as arguments
  pub fn init(height: u8) Commiter {
    return Commiter{ .height = height, .levels = Commiter.DafaultLevels };
  }

  pub fn prune(self: *Commiter, tree: *Tree) bool {
    return self.height - tree.height() >= self.levels;
  }

  pub fn write(tree: *Tree) void {
    var buf: [1024]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    tree.marshal(fbs.writer()) catch unreachable;

    DB.putBatch(tree.hash().inner[0..], fbs.getWritten()) catch unreachable;
  }
};

test "write" {
  try DB.open();
  defer DB.close();

  try DB.createBatch();
  defer DB.destroyBatch();

  var commiter = Commiter.init(1);

  var tree = Tree.init("key", "value");
  Commiter.write(tree);

  var buf_m: [1024]u8 = undefined;
  var fbs_m = std.io.fixedBufferStream(&buf_m);
  try tree.marshal(fbs_m.writer());

  try DB.write();

  const key = tree.hash().inner[0..];

  var buf: [1024]u8 = undefined;
  var fbs = std.io.fixedBufferStream(&buf);
  _= try DB.read(key, fbs.writer());

  testing.expectEqualSlices(u8, fbs.getWritten(), fbs_m.getWritten());
}
