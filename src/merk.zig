const std = @import("std");
const testing = std.testing;
const Tree = @import("tree.zig").Tree;
const ops = @import("ops.zig");
const Op = ops.Op;
const OpTag = ops.OpTag;
const DB = @import("db.zig").DB;
const root_key = @import("db.zig").root_key;
const Hash = @import("hash.zig").Hash;
const ZeroHash = @import("hash.zig").ZeroHash;
const Commiter = @import("commit.zig").Commiter;
const LinkTag = @import("link.zig").LinkTag;

const BatchError = error {
  InvalidOrder,
  Invalid,
};

pub const Merk = struct {
  tree: ?*Tree = null,

  pub fn init() !Merk {
    var merk: Merk = Merk{ .tree = null };
    try DB.open();

    var buf: [1024]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    var w = fbs.writer();
    _= try DB.read(root_key, w);

    const top_key = fbs.getWritten();
    if (top_key.len == 0) {
      return merk;
    }
    var tree = Tree.fetchTrees(top_key);
    merk.tree = tree;
    return merk;
  }

  pub fn rootHash(self: *Merk) Hash {
    if (self.tree) |tree| {
      return tree.hash();
    } else {
      return ZeroHash;
    }
  }

  pub fn apply(self: *Merk, batch: []Op) !void {
    var pre_key: []const u8 = "";

    for (batch) |op| {
      if (std.mem.lessThan(u8, op.key, pre_key)) {
        std.debug.print("keys in batch must be sorted\n", .{});
        return BatchError.InvalidOrder;
      } else if (std.mem.eql(u8, op.key, pre_key)) {
        std.debug.print("keys in batch must be unique, {}\n", .{op.key});
        return BatchError.Invalid;
      }

      pre_key = op.key;
    }

    self.applyUnchecked(batch);
  }

  pub fn applyUnchecked(self: *Merk, batch: []Op) void {
    self.tree = ops.applyTo(self.tree, batch);
  }

  pub fn commit(self: *Merk) !void {
    try DB.createBatch();
    defer DB.destroyBatch();

    if (self.tree) |tree| {
      var commiter = Commiter.init(tree.height());
      tree.commit(&commiter);
      try DB.putBatch(root_key, self.rootHash().inner[0..]);
    } else {
      // TODO: delete root key
    }

    try DB.write();
  }
};


test "init" {
  var merk = try Merk.init();
  defer DB.close();
}

// test "commit and fetch" {
//   var merk = try Merk.init();
//   defer DB.close();

//   DB.destroy();

//   merk.tree = null;

//   var op0 = Op{ .op = OpTag.Put, .key = "key0", .val = "value0" };
//   var op1 = Op{ .op = OpTag.Put, .key = "key1", .val = "value1" };
//   var op2 = Op{ .op = OpTag.Put, .key = "key2", .val = "value2" };

//   var batch = [_]Op{op0, op1, op2};

//   try merk.apply(&batch);
//   try merk.commit();

//   testing.expect(merk.tree.?.verify());

//   var top_key = merk.rootHash().inner[0..];
//   std.debug.print("top_key: {x}\n", .{top_key});
//   var tree = Tree.fetchTrees(top_key);
//   std.debug.print("====================\n\n", .{});
//   std.debug.print("fetch: {}\n", .{tree.key()});
//   std.debug.print("fetch: {}\n", .{tree.child(true).?.key()});
//   std.debug.print("fetch: {}\n", .{tree.child(false).?.key()});

// }

test "apply and commit" {
  var merk = try Merk.init();
  defer DB.close();

  DB.destroy();
  merk.tree = null;

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

  var batch = [_]Op{op0, op1, op2, op3, op4, op5, op6, op7, op8, op9};

  try merk.apply(&batch);

  // std.debug.print("applyed: {}\n", .{merk.tree.?.key()});
  // std.debug.print("applyed: {}\n", .{merk.tree.?.child(true).?.key()});
  // std.debug.print("applyed: {}\n", .{merk.tree.?.child(true).?.child(true).?.key()});
  // std.debug.print("applyed: {}\n", .{merk.tree.?.child(true).?.child(false).?.key()});
  // std.debug.print("applyed: {}\n", .{merk.tree.?.child(true).?.child(false).?.child(true).?.key()});
  // std.debug.print("applyed: {}\n", .{merk.tree.?.child(true).?.child(true).?.child(true).?.key()});
  // std.debug.print("applyed: {}\n", .{merk.tree.?.child(false).?.key()});
  // std.debug.print("applyed: {}\n", .{merk.tree.?.child(false).?.child(true).?.key()});
  // std.debug.print("applyed: {}\n", .{merk.tree.?.child(false).?.child(true).?.child(true).?.key()});
  // std.debug.print("applyed: {}\n", .{merk.tree.?.child(false).?.child(false).?.key()});

  try merk.commit();

  testing.expect(merk.tree.?.verify());
  testing.expectEqual(@as(LinkTag, merk.tree.?.child(true).?.link(true).?), .Pruned);
  testing.expectEqual(@as(LinkTag, merk.tree.?.child(true).?.link(false).?), .Pruned);
  testing.expectEqual(@as(LinkTag, merk.tree.?.child(false).?.link(true).?), .Pruned);
  testing.expectEqual(@as(LinkTag, merk.tree.?.child(false).?.link(false).?), .Pruned);
}


test "apply" {
  var merk: *Merk = undefined;

  var op0 = Op{ .op = OpTag.Put, .key = "key0", .val = "value" };
  var op1 = Op{ .op = OpTag.Put, .key = "key1", .val = "value" };
  var op2 = Op{ .op = OpTag.Put, .key = "key2", .val = "value" };

  var batch1 = [_]Op{op0, op2, op1};
  testing.expectError(BatchError.InvalidOrder, merk.apply(&batch1));

  var batch2 = [_]Op{op0, op2, op2};
  testing.expectError(BatchError.Invalid, merk.apply(&batch2));
}
