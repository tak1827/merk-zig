const std = @import("std");
const c = @cImport(@cInclude("rocksdb/c.h"));
const testing = std.testing;

pub const root_key = ".root";
pub const node_key_prefix = "@1:";
const default_db_dir = "./db";

pub const RocksDataBbase = DB(RocksDB);

pub fn DB(comptime T: type) type {
    return struct {
        const Self = @This();
        db: T,

        pub fn init(dir: ?[]const u8) !Self {
            var ctx: Self = undefined;
            ctx.db = try T.init(dir);
            return ctx;
        }

        pub fn deinit(self: Self) void {
            self.db.deinit();
        }

        pub fn put(self: *Self, key: []const u8, val: []const u8) void {
            self.db.put(key, val);
        }

        pub fn clear(self: Self) void {
            self.db.clear();
        }

        pub fn commit(self: *Self) !void {
            try self.db.commit();
        }

        pub fn read(self: *Self, key: []const u8, w: anytype) !usize {
            return try self.db.read(key, w);
        }

        pub fn destroy(self: *Self, dir: ?[]const u8) void {
            self.db.destroy(dir);
        }
    };
}

pub const RocksDB = struct {
    db: *c.rocksdb_t,
    batch: *c.rocksdb_writebatch_t,

    pub fn init(dir: ?[]const u8) !RocksDB {
        var rockdb: RocksDB = undefined;

        // opendb
        const opts = c.rocksdb_options_create();
        defer c.rocksdb_options_destroy(opts);
        c.rocksdb_options_increase_parallelism(opts, 8);
        c.rocksdb_options_optimize_level_style_compaction(opts, @boolToInt(false));
        c.rocksdb_options_set_create_if_missing(opts, @boolToInt(true));
        var err: ?[*:0]u8 = null;
        rockdb.db = if (dir) |d| c.rocksdb_open(opts, @ptrCast([*:0]const u8, d), &err).? else c.rocksdb_open(opts, default_db_dir, &err).?;
        if (err) |message| {
            std.debug.print("failed to open rockdb, {}\n", .{std.mem.spanZ(message)});
            return error.FaildOpen;
        }

        // create batch
        rockdb.batch = c.rocksdb_writebatch_create().?;
        return rockdb;
    }

    pub fn deinit(self: RocksDB) void {
        self.clear();
        c.rocksdb_close(self.db);
    }

    pub fn put(self: *RocksDB, key: []const u8, val: []const u8) void {
        c.rocksdb_writebatch_put(self.batch, @ptrCast([*]const u8, key), key.len, @ptrCast([*]const u8, val), val.len);
    }

    pub fn clear(self: RocksDB) void {
        c.rocksdb_writebatch_destroy(self.batch);
    }

    pub fn commit(self: *RocksDB) !void {
        const write_opts = c.rocksdb_writeoptions_create();
        var err: ?[*:0]u8 = null;
        c.rocksdb_write(self.db, write_opts, self.batch, &err);
        if (err) |message| {
            std.debug.print("faild to commit to rockdb, {}\n", .{std.mem.spanZ(message)});
            return error.FaildCommit;
        }
    }

    pub fn read(self: *RocksDB, key: []const u8, w: anytype) !usize {
        const read_opts = c.rocksdb_readoptions_create();
        defer c.rocksdb_readoptions_destroy(read_opts);

        var read_len: usize = undefined;
        var err: ?[*:0]u8 = null;
        const c_key = @ptrCast([*:0]const u8, key);
        var read_ptr = c.rocksdb_get(self.db, read_opts, c_key, key.len, &read_len, &err);
        if (err) |message| {
            std.debug.print("faild to commit to rockdb, {}\n", .{std.mem.spanZ(message)});
            return error.FailedRead;
        }
        if (read_len == 0) return 0;
        defer std.c.free(read_ptr);
        try w.writeAll(read_ptr[0..read_len]);

        return read_len;
    }

    pub fn destroy(self: *RocksDB, dir: ?[]const u8) void {
        if (dir) |d| {
            c.rocksdb_delete_file(self.db, @ptrCast([*:0]const u8, d));
        } else {
            c.rocksdb_delete_file(self.db, default_db_dir);
        }
    }
};

test "init" {
    var db = try DB(RocksDB).init("dbtest");
    defer db.deinit();
    defer db.destroy("dbtest");

    var key = "testkey";
    var value = "testvalue";
    db.put(key, value);
    try db.commit();

    var buf: [1024]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    _ = try db.read(key, fbs.writer());
    testing.expectEqualSlices(u8, fbs.getWritten(), value);
}
