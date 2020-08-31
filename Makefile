test:
	zig test src/test.zig --library rocksdb

bench:
	zig test --library rocksdb --release-fast src/test.zig

fmt:
	zig fmt src/*
