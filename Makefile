.PHONY: test interop-test pbt check clippy fmt fuzzing fuzzing-list clean

# 全テストを実行する
test:
	cargo test --workspace

# interop テスト (dwnx との疎通確認) を実行する
interop-test:
	cargo test -p interop

# PBT を実行する
pbt:
	cargo test -p pbt

# Fuzzing を全ターゲットで 30 秒ずつ実行する
fuzzing:
	@for target in $$(cargo fuzz list); do \
		echo "=== Fuzzing $$target ==="; \
		cargo +nightly fuzz run $$target -- -max_total_time=30 || exit 1; \
	done

# Fuzzing ターゲット一覧を表示する
fuzzing-list:
	cargo fuzz list

# cargo check を実行する
check:
	cargo check --workspace

# cargo clippy を実行する
clippy:
	cargo clippy --workspace --all-targets -- -D warnings

# cargo fmt を実行する
fmt:
	cargo fmt --all

# ビルド成果物を削除する
clean:
	cargo clean
