const std = @import("std");
const testing = std.testing;
const crypto = std.crypto;
const Allocator = std.mem.Allocator;

pub const HashBlake2s256 = Hash(crypto.Blake3);

pub fn Hash(comptime T: type) type {
    return struct {
        const Self = @This();
        inner: [T.digest_length]u8,

        pub fn init(key: []const u8) Self {
            var ctx: Self = undefined;
            T.hash(key, &ctx.inner);
            return ctx;
        }

        pub fn initPtr(allocator: *Allocator, key: []const u8) !*Self {
            var ctx = try allocator.create(Self);
            errdefer allocator.destroy(ctx);
            T.hash(key, &ctx.inner);
            return ctx;
        }

        pub fn update(ctx: *Self, key: []const u8) void {
            T.hash(key, &ctx.inner);
        }

        pub fn len() usize {
            return T.digest_length;
        }

        pub fn zeroHash() Self {
            var ctx: Self = undefined;
            ctx.inner = [1]u8{0} ** T.digest_length;
            return ctx;
        }
    };
}

test "init & update" {
    HashBlake2s256.init("key").update("key2");
}

test "zeroHash" {
    var h = HashBlake2s256.zeroHash();
    var expected = [crypto.Blake2s256.digest_length]u8{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    testing.expectEqualSlices(u8, &h.inner, &expected);
}

test "len" {
    const expected: usize = 32;
    testing.expectEqual(HashBlake2s256.len(), expected);
}
