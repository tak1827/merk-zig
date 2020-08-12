const std = @import("std");
const testing = std.testing;
const fmt = std.fmt;
const rand = std.rand;
const mem = std.mem;
const Allocator = std.mem.Allocator;

pub fn concat(allocator: *Allocator, bufs: []const []const u8) []const u8 {
    if (std.mem.concat(allocator, u8, bufs)) |concated| {
        return concated;
    } else |err| {
        std.debug.print("error while concatnating, {}\n", .{err});
        @panic("failed to concatnate");
    }
}

pub fn intToString(buf: []u8, value: anytype) []u8 {
    return buf[0..fmt.formatIntBuf(buf, value, 10, false, fmt.FormatOptions{})];
}

pub fn randRepeatString(output: []u8, comptime base: comptime_int, comptime max: comptime_int, comptime T: type, seed: u64) T {
    const repeated = [1]u8{base} ** max;
    var r = rand.DefaultPrng.init(seed);
    var len = r.random.int(T);
    mem.copy(u8, output, repeated[0..len]);
    return len;
}

test "concat" {
    const key: []const u8 = "key";
    const val: []const u8 = "value";
    var concated = concat(testing.allocator, &[2][]const u8{ key, val });
    defer testing.allocator.free(concated);

    testing.expectEqualSlices(u8, "keyvalue", concated);
}

test "intToString" {
    var buffer: [100]u8 = undefined;
    testing.expectEqualSlices(u8, "1", intToString(&buffer, @as(u8, 1)));

    var i : usize = 1000;
    testing.expectEqualSlices(u8, "1000", intToString(&buffer, @as(u64, i)));
}

test "randRepeatString" {
    const base: comptime_int = 97;
    const max: comptime_int = 1024;

    var buf: [1024]u8 = undefined;
    var len = randRepeatString(&buf, base, max, u10, 0);
    testing.expectEqualSlices(u8, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", buf[0..len]);
}