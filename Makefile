test:
  zig test src/util.zig

db:
	zig run src/db.zig --library rocksdb
