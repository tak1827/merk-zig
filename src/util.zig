const std = @import("std");
const testing = std.testing;

pub fn concat(bufs: []const []const u8) []const u8 {
  if (std.mem.concat(std.heap.c_allocator, u8, bufs)) |concated| {
    return concated;
  } else |err| {
    std.debug.print("error while concatnating, {}\n", .{err});
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
