const std = @import("std");
const testing = std.testing;
const warn = std.debug.warn;

pub fn concat(bufs: []const []const u8) []const u8 {
  if (std.mem.concat(std.heap.c_allocator, u8, bufs)) |concated| {
    return concated;
  } else |err| {
    @panic("failed to concatnate");
  }
}

test "concat" {
  const key: []const u8 = "key";
  const val: []const u8 = "value";
  var concated = concat(&[2][]const u8{key, val});
  defer std.heap.c_allocator.free(concated);

  testing.expectEqualSlices(u8, "keyvalue", concated);
}
