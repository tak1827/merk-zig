const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;

pub fn concat(allocator: *Allocator, bufs: []const []const u8) []const u8 {
  if (std.mem.concat(allocator, u8, bufs)) |concated| {
    return concated;
  } else |err| {
    std.debug.print("error while concatnating, {}\n", .{err});
    @panic("failed to concatnate");
  }
}

test "concat" {
  const key: []const u8 = "key";
  const val: []const u8 = "value";
  var concated = concat(testing.allocator, &[2][]const u8{key, val});
  defer testing.allocator.free(concated);

  testing.expectEqualSlices(u8, "keyvalue", concated);
}
