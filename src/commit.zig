const std = @import("std");
const testing = std.testing;
const c = @cImport(@cInclude("rocksdb/c.h"));
const Tree = @import("tree.zig").Tree;
const o = @import("ops.zig");
const Op = o.Op;
const OpTag = o.OpTag;
const DB = @import("db.zig").RocksDataBbase;
const Merk = @import("merk.zig").Merk;

pub const Commiter = struct {
    db: DB,
    height: u8,
    levels: u8,

    // TODO: change as config
    pub const DafaultLevels: u8 = 1;

    // TODO: pass level as arguments
    pub fn init(db: DB, height: u8) !Commiter {
        return Commiter{
            .db = db,
            .height = height,
            .levels = Commiter.DafaultLevels,
        };
    }

    pub fn put(self: *Commiter, key: []const u8, val: []const u8) void {
        self.db.put(key, val);
    }

    pub fn write(self: *Commiter, tree: *Tree) void {
        var allocator = if (Merk.heap_allocator) |_| &Merk.heap_allocator.?.allocator else Merk.stack_allocator;
        var buf = std.ArrayList(u8).init(allocator);
        tree.marshal(buf.writer()) catch unreachable;
        defer buf.deinit();

        self.db.put(tree.key(), buf.toOwnedSlice());
    }

    pub fn commit(self: *Commiter) !void {
        try self.db.commit();
    }

    // Note: disable this function, but keep this for future use case
    pub fn prune(self: *Commiter, tree: *Tree) bool {
        return self.height - tree.height() >= self.levels;
    }
};

test "write" {
    var db = try DB.init("dbtest");
    defer db.destroy("dbtest");
    defer db.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    Merk.stack_allocator = &arena.allocator;
    defer arena.deinit();

    var commiter = try Commiter.init(db, 1);
    var tree = try Tree.init("key", "value");
    commiter.write(tree);
    try commiter.commit();
    var feched = Tree.fetchTree(db, tree.key());

    testing.expectEqualSlices(u8, tree.key(), feched.key());
    testing.expectEqualSlices(u8, tree.value(), feched.value());
}
