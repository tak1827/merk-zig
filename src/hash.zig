const std = @import("std");
const testing = std.testing;
const crypto = std.crypto;

pub const HashBlake2s256 = Hash(crypto.Blake2s256);

pub fn Hash(comptime T: type) type {
    return struct {
        const Self = @This();
        inner: [T.digest_length]u8,

        pub fn init(key: []const u8) Self {
            var ctx: Self = undefined;
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
            var i: usize = 0;
            while (i < T.digest_length) {
                ctx.inner[i] = 0;
                i += 1;
            }
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
