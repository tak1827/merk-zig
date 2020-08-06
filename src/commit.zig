const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const c = @cImport(@cInclude("rocksdb/c.h"));
const Tree = @import("tree.zig").Tree;
const o = @import("ops.zig");
const Op = o.Op;
const OpTag = o.OpTag;
const db = @import("db.zig");
const DB = db.DB;

pub const Commiter = struct {
    allocator: *Allocator,
    height: u8,
    levels: u8,

    const DafaultLevels: u8 = 1;

    // TODO: pass level as arguments
    pub fn init(allocator: *Allocator, height: u8) !Commiter {
        try DB.createBatch();
        return Commiter{ .allocator = allocator, .height = height, .levels = Commiter.DafaultLevels };
    }

    pub fn deinit(self: Commiter) void {
        DB.destroyBatch();
    }

    pub fn prune(self: *Commiter, tree: *Tree) bool {
        return self.height - tree.height() >= self.levels;
    }

    pub fn write(self: *Commiter, tree: *Tree) void {
        var buf = std.ArrayList(u8).init(self.allocator);
        tree.marshal(buf.writer()) catch unreachable;
        defer buf.deinit();

        DB.putBatch(tree.hash().inner[0..], buf.toOwnedSlice()) catch unreachable;
    }
};

test "write" {
    try DB.open("dbtest");
    defer DB.close();

    var buf: [65536]u8 = undefined;
    var buffer = std.heap.FixedBufferAllocator.init(&buf);
    var arena = std.heap.ArenaAllocator.init(&buffer.allocator);
    defer arena.deinit();

    var commiter = try Commiter.init(arena.child_allocator, 1);
    defer commiter.deinit();

    var tree = try Tree.init(arena.child_allocator, "key", "value");
    commiter.write(tree);

    try DB.write();
}
