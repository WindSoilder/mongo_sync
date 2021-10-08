build-release:
	cargo +nightly build --release

release-mac: build-release
	strip target/release/oplog_syncer
	strip target/release/db_sync
	mkdir -p release
	tar -C ./target/release/ -czvf ./release/mongo_sync-mac.tar.gz ./oplog_syncer ./db_sync
	ls -lisah ./release/mongo_sync-mac.tar.gz

release-win: build-release
	mkdir -p release
	tar -C ./target/release/ -czvf ./release/mongo_sync-win.tar.gz ./oplog_syncer.exe ./db_sync.exe

release-ubuntu: build-release
	strip target/release/oplog_syncer
	strip target/release/db_sync
	mkdir -p release
	tar -C ./target/release/ -czvf ./release/mongo_sync-ubuntu.tar.gz ./oplog_syncer ./db_sync
	ls -lisah ./release/mongo_sync-ubuntu.tar.gz

release-linux-musl:
	cargo +nightly build --release --target=x86_64-unknown-linux-musl
	strip target/x86_64-unknown-linux-musl/release/oplog_syncer
	strip target/x86_64-unknown-linux-musl/release/db_sync
	mkdir -p release
	tar -C ./target/x86_64-unknown-linux-musl/release/ -czvf ./release/mongo_sync-linux-musl.tar.gz ./oplog_syncer ./db_sync
