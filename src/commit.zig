const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const c = @cImport(@cInclude("rocksdb/c.h"));
const Tree = @import("tree.zig").Tree;
const o = @import("ops.zig");
const Op = o.Op;
const OpTag = o.OpTag;
const DB = @import("db.zig").RocksDataBbase;

pub const Commiter = struct {
    allocator: *Allocator,
    db: *DB,
    height: u8,
    levels: u8,

    const DafaultLevels: u8 = 1;

    // TODO: pass level as arguments
    pub fn init(allocator: *Allocator, db: *DB, height: u8) !Commiter {
        return Commiter{
            .allocator = allocator,
            .db = db,
            .height = height,
            .levels = Commiter.DafaultLevels,
        };
    }

    pub fn put(self: *Commiter, key: []const u8, val: []const u8) void {
        self.db.put(key, val);
    }

    pub fn write(self: *Commiter, tree: *Tree) void {
        var buf = std.ArrayList(u8).init(self.allocator);
        tree.marshal(buf.writer()) catch unreachable;
        defer buf.deinit();

        self.db.put(tree.key(), buf.toOwnedSlice());
    }

    pub fn commit(self: *Commiter) !void {
        try self.db.commit();
    }

    pub fn prune(self: *Commiter, tree: *Tree) bool {
        return self.height - tree.height() >= self.levels;
    }
};

test "write" {
    var db = try DB.init("dbtest");
    defer db.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    var commiter = try Commiter.init(&arena.allocator, &db, 1);
    var tree = try Tree.init(&arena.allocator, &db, "key", "value");
    commiter.write(tree);
    try commiter.commit();
    var feched = Tree.fetchTree(&arena.allocator, &db, tree.key());

    testing.expectEqualSlices(u8, tree.key(), feched.key());
    testing.expectEqualSlices(u8, tree.value(), feched.value());
}
