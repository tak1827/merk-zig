const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const mem = std.mem;
const Hash = @import("hash.zig").HashBlake2s256;
const util = @import("util.zig");

pub const KV = struct {
    key: []const u8,
    val: []const u8,
    hash: Hash,

    pub fn init(allocator: *Allocator, key: []const u8, val: []const u8) KV {
        return KV{ .key = key, .val = val, .hash = KV.kvHash(allocator, key, val) };
    }

    pub fn kvHash(allocator: *Allocator, key: []const u8, val: []const u8) Hash {
        var kv = util.concat(allocator, &[2][]const u8{ key, val });
        defer allocator.free(kv);
        return Hash.init(kv);
    }

    pub fn nodeHash(allocator: *Allocator, kv: Hash, left: Hash, right: Hash) Hash {
        var h: Hash = undefined;
        var concated = util.concat(allocator, &[2][]const u8{ kv.inner[0..], left.inner[0..] });
        defer allocator.free(concated);
        concated = util.concat(allocator, &[2][]const u8{ concated, right.inner[0..] });
        defer allocator.free(concated);

        return Hash.init(concated);
    }
};

test "init" {
    const kv = KV.init(testing.allocator, "key", "value");
    const expected = Hash.init("keyvalue");

    testing.expectEqualSlices(u8, kv.key, "key");
    testing.expectEqualSlices(u8, kv.val, "value");
    testing.expectEqualSlices(u8, &kv.hash.inner, &expected.inner);
}

test "Hash" {
    const kv: Hash = KV.kvHash(testing.allocator, "key", "value");
    const expected = Hash.init("keyvalue");

    testing.expectEqualSlices(u8, &kv.inner, &expected.inner);
}

test "nodeHash" {
    const kv = KV.kvHash(testing.allocator, "key", "value");
    const left = KV.kvHash(testing.allocator, "lkey", "lvalue");
    const right = KV.kvHash(testing.allocator, "rkey", "rvalue");
    _ = KV.nodeHash(testing.allocator, kv, left, right);
}
