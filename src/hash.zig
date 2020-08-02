const std = @import("std");
const warn = std.debug.warn;
const assert = std.debug.assert;
const mem = std.mem;
const util = @import("util.zig");

pub const ZeroHash: Hash = Hash{ .inner = [32]u8{0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0} };

pub const HashLen = std.crypto.Blake2s256.digest_length;

pub const Hash = struct {
  inner: [HashLen]u8,
};

pub fn kvHash(key: []const u8, val: []const u8) Hash {
  var h: Hash = undefined;
  var kv = util.concat(&[2][]const u8{key, val});
  defer std.heap.c_allocator.free(kv);

  std.crypto.Blake2s256.hash(kv, &h.inner);
  return h;
}

pub fn nodeHash(kv: Hash, left: Hash, right: Hash) Hash {
  var h: Hash = undefined;
  var concated = util.concat(&[2][]const u8{kv.inner[0..], left.inner[0..]});
  concated = util.concat(&[2][]const u8{concated, right.inner[0..]});
  defer std.heap.c_allocator.free(concated);

  std.crypto.Blake2s256.hash(concated[0..], &h.inner);
  return h;
}

test "kvHash" {
  const kv: Hash = kvHash("key", "value");
  var expected: [32]u8 = undefined;
  std.crypto.Blake2s256.hash("keyvalue", &expected);

  assert(mem.eql(u8, kv.inner[0..], expected[0..]));
}

// TODO: change smart way to confirm no error
test "nodeHash" {
  const kv = kvHash("key", "value");
  const left = kvHash("lkey", "lvalue");
  const right = kvHash("rkey", "rvalue");
  const h = nodeHash(kv, left, right);
}
