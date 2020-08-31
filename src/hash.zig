const std = @import("std");
const testing = std.testing;
const crypto = std.crypto;
const Allocator = std.mem.Allocator;

pub const H = Hash(std.hash.Wyhash);

pub fn Hash(comptime T: type) type {
    return struct {
        const Self = @This();
        inner: [8]u8,

        pub fn init(key: []const u8) Self {
            var ctx: Self = undefined;
            var result = T.hash(key.len, key);
            var buf: [8]u8 = undefined;
            var fbs = std.io.fixedBufferStream(&buf);
            fbs.writer().writeIntBig(u64, result) catch unreachable;
            std.mem.copy(u8, &ctx.inner, fbs.getWritten());
            return ctx;
        }

        pub fn len() usize {
            return 8;
        }

        pub fn zeroHash() Self {
            var ctx: Self = undefined;
            ctx.inner = [1]u8{0} ** 8;
            return ctx;
        }
    };
}

test "init & update" {
    _ = H.init("key");
}

test "zeroHash" {
    var h = H.zeroHash();
    var expected = [8]u8{ 0, 0, 0, 0, 0, 0, 0, 0 };
    testing.expectEqualSlices(u8, &h.inner, &expected);
}

test "len" {
    const expected: usize = 8;
    testing.expectEqual(H.len(), expected);
}
