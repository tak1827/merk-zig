const std = @import("std");
const Allocator = std.mem.Allocator;

// Note: change hash function case by case
pub const HashFunc = std.crypto.Blake2s256;
pub const HashLen = std.crypto.Blake2s256.digest_length;
pub const ZeroHash: Hash = Hash{ .inner = [HashLen]u8{0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0} };
pub const Hash = struct { inner: [HashLen]u8 };
