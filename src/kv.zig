const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const mem = std.mem;
const Hash = @import("hash.zig").HashBlake2s256;
const util = @import("util.zig");
const o = @import("ops.zig");

pub const KV = struct {
    key: []const u8,
    val: []const u8,
    hash: Hash,

    // TODO: remove allocator
    pub fn init(key: []const u8, val: []const u8) KV {
        return KV{ .key = key, .val = val, .hash = KV.kvHash(key, val) };
    }

    pub fn kvHash(key: []const u8, val: []const u8) Hash {
        var buf: [o.BatchKeyLimit+o.BatchValueLimit]u8 = undefined;
        var buffer = std.heap.FixedBufferAllocator.init(&buf);
        var kv = util.concat(&buffer.allocator, &[2][]const u8{ key, val });
        return Hash.init(kv);
    }

    pub fn nodeHash(kv: Hash, left: Hash, right: Hash) Hash {
        var buf: [32*10]u8 = undefined;
        var buffer = std.heap.FixedBufferAllocator.init(&buf);
        var concated = util.concat(&buffer.allocator, &[2][]const u8{ kv.inner[0..], left.inner[0..] });
        concated = util.concat(&buffer.allocator, &[2][]const u8{ concated, right.inner[0..] });
        return Hash.init(concated);
    }
};

test "init" {
    const kv = KV.init("key", "value");
    const expected = Hash.init("keyvalue");

    testing.expectEqualSlices(u8, kv.key, "key");
    testing.expectEqualSlices(u8, kv.val, "value");
    testing.expectEqualSlices(u8, &kv.hash.inner, &expected.inner);
}

test "Hash" {
    const kv: Hash = KV.kvHash("key", "value");
    const expected = Hash.init("keyvalue");

    testing.expectEqualSlices(u8, &kv.inner, &expected.inner);
}

test "nodeHash" {
    const kv = KV.kvHash("key", "value");
    const left = KV.kvHash("lkey", "lvalue");
    const right = KV.kvHash("rkey", "rvalue");
    _ = KV.nodeHash(kv, left, right);
}
