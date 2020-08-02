const std = @import("std");
const c = @cImport(@cInclude("rocksdb/c.h"));
const testing = std.testing;

// TODO: consider to move DB struct
pub var merk_db: ?*c.rocksdb_t = null;
pub var merk_batch: ?*c.rocksdb_writebatch_t = null;

pub const db_name = "db";
pub const root_key = ".root";
pub const node_key_prefix = "@1:";

const DBError = error {
  DuplicatedOpen,
  NotOpen,
  DuplicatedBatch,
  NoBatch,
};

pub const DB = struct {

  // TODO: specify directry
  pub fn open() !void {
    if (merk_db) |_| return DBError.DuplicatedOpen;

    // TODO: custom options
    const opts = c.rocksdb_options_create();
    defer c.rocksdb_options_destroy(opts);

    c.rocksdb_options_increase_parallelism(opts, 8);
    c.rocksdb_options_optimize_level_style_compaction(opts, @boolToInt(false));
    c.rocksdb_options_set_create_if_missing(opts, @boolToInt(true));

    var err: ?[*:0]u8 = null;
    merk_db = c.rocksdb_open(opts, db_name, &err);
    if (err) |message| @panic(std.mem.spanZ(message));
  }

  pub fn destroy() void {
    if (merk_db) |db| c.rocksdb_delete_file(db, db_name);
  }

  pub fn close() void {
    if (merk_db) |db| {
      c.rocksdb_close(db);
      merk_db = null;
    }
  }

  pub fn write() !void {
    if (merk_db) |db| {
      const write_opts = c.rocksdb_writeoptions_create();
      var err: ?[*:0]u8 = null;
      c.rocksdb_write(db, write_opts, merk_batch, &err);
      if (err) |message| @panic(std.mem.spanZ(message));
    } else {
      return DBError.NotOpen;
    }
  }

  // TODO: use an allocator
  pub fn read(key: []const u8, w: anytype) !usize {
    if (merk_db) |db| {
      const read_opts = c.rocksdb_readoptions_create();
      defer c.rocksdb_readoptions_destroy(read_opts);

      var read_len: usize = undefined;
      var err: ?[*:0]u8 = null;
      const c_key = @ptrCast([*:0]const u8, key);
      var read_ptr = c.rocksdb_get(db, read_opts, c_key, key.len, &read_len, &err);
      if (err) |message| @panic(std.mem.spanZ(message));
      if (read_len == 0) return 0;
      defer std.c.free(read_ptr);
      try w.writeAll(read_ptr[0..read_len]);

      return read_len;
    } else {
      return DBError.NotOpen;
    }
  }

  pub fn createBatch() !void {
    if (merk_batch) |_| return DBError.DuplicatedBatch;
    merk_batch = c.rocksdb_writebatch_create();
  }

  pub fn putBatch(key: []const u8, val: []const u8) !void {
    if (merk_batch) |batch| {
      return c.rocksdb_writebatch_put(batch, @ptrCast([*]const u8, key), key.len, @ptrCast([*]const u8, val), val.len);
    }
    return DBError.NoBatch;
  }

  pub fn destroyBatch() void {
    if (merk_batch) |batch| {
      c.rocksdb_writebatch_destroy(batch);
      merk_batch = null;
    }
  }
};

test "batch" {
  try DB.open();
  defer DB.close();

  try DB.createBatch();
  defer DB.destroyBatch();

  const key = "key0";
  const val = "value0";
  const keyX = "keyX";
  const valX = "valueX";
  try DB.putBatch(key, val);
  try DB.putBatch(keyX, valX);

  try DB.write();

  var buf: [1024]u8 = undefined;
  var fbs = std.io.fixedBufferStream(&buf);
  var w = fbs.writer();
  _ = try DB.read(key, w);
  testing.expectEqualSlices(u8, fbs.getWritten(), val);
  fbs.reset();
  _ = try DB.read(keyX, w);
  testing.expectEqualSlices(u8, fbs.getWritten(), valX);
}
