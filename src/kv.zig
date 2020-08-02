const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const h = @import("hash.zig");
const Hash = h.Hash;

pub const KV = struct {
  key: []const u8,
  val: []const u8,
  hash: Hash,

  pub fn init(key: []const u8, val: []const u8) KV {
    return KV{ .key = key, .val = val, .hash = h.kvHash(key, val) };
  }
};

test "init" {
  const kv = KV.init("key", "value");
  var expected: [32]u8 = undefined;
  std.crypto.Blake2s256.hash("keyvalue", &expected);

  assert(mem.eql(u8, kv.key[0..], "key"));
  assert(mem.eql(u8, kv.val[0..], "value"));
  assert(mem.eql(u8, kv.hash.inner[0..], expected[0..]));
}
