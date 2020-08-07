const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const mem = std.mem;
const HashFunc = @import("hash.zig").HashFunc;
const Hash = @import("hash.zig").Hash;
const util = @import("util.zig");

pub const KV = struct {
    key: []const u8,
    val: []const u8,
    hash: Hash,

    pub fn init(allocator: *Allocator, key: []const u8, val: []const u8) KV {
        return KV{ .key = key, .val = val, .hash = KV.kvHash(allocator, key, val) };
    }

    pub fn kvHash(allocator: *Allocator, key: []const u8, val: []const u8) Hash {
        var h: Hash = undefined;
        var kv = util.concat(allocator, &[2][]const u8{ key, val });
        defer allocator.free(kv);

        HashFunc.hash(kv, &h.inner);
        return h;
    }

    pub fn nodeHash(allocator: *Allocator, kv: Hash, left: Hash, right: Hash) Hash {
        var h: Hash = undefined;
        var concated = util.concat(allocator, &[2][]const u8{ kv.inner[0..], left.inner[0..] });
        defer allocator.free(concated);
        concated = util.concat(allocator, &[2][]const u8{ concated, right.inner[0..] });
        defer allocator.free(concated);

        HashFunc.hash(concated[0..], &h.inner);
        return h;
    }
};

test "init" {
    const kv = KV.init(testing.allocator, "key", "value");
    var expected: [32]u8 = undefined;
    HashFunc.hash("keyvalue", &expected);

    testing.expectEqualSlices(u8, kv.key, "key");
    testing.expectEqualSlices(u8, kv.val, "value");
    testing.expectEqualSlices(u8, &kv.hash.inner, &expected);
}

test "Hash" {
    const kv: Hash = KV.kvHash(testing.allocator, "key", "value");
    var expected: [32]u8 = undefined;
    HashFunc.hash("keyvalue", &expected);

    testing.expectEqualSlices(u8, kv.inner[0..], expected[0..]);
}

test "nodeHash" {
    const kv = KV.kvHash(testing.allocator, "key", "value");
    const left = KV.kvHash(testing.allocator, "lkey", "lvalue");
    const right = KV.kvHash(testing.allocator, "rkey", "rvalue");
    _ = KV.nodeHash(testing.allocator, kv, left, right);
}
