.PHONY: test interop-test pbt check clippy fmt fmt-fix doc coverage fuzz-check fuzz-clippy fuzz-fmt fuzzing fuzzing-list clean

# 全テストを実行する
test:
	cargo test --workspace

# interop テスト (dwnx との疎通確認) を実行する
interop-test:
	cargo test -p interop

# PBT を実行する
pbt:
	cargo test -p pbt

# Fuzzing を全ターゲットで 30 秒ずつ実行する (nightly と cargo-fuzz が必要)
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
	cargo check --workspace --all-targets

# cargo clippy を実行する
clippy:
	cargo clippy --workspace --all-targets -- -D warnings

# cargo fmt で整形の検査をする
fmt:
	cargo fmt --all -- --check

# cargo fmt で整形する
fmt-fix:
	cargo fmt --all

# CI の rustdoc 検査 (RUSTDOCFLAGS=-D warnings) を再現する
doc:
	RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --exclude dwnx-sys

# カバレッジを計測する (cargo-llvm-cov と llvm-tools コンポーネントが必要)
coverage:
	cargo llvm-cov --workspace

# fuzz クレートの fmt 検査、check、clippy を実行する (CI の「Check fuzz crate」に対応)
fuzz-check:
	cargo fmt --manifest-path fuzz/Cargo.toml -- --check
	cargo check --manifest-path fuzz/Cargo.toml
	cargo clippy --manifest-path fuzz/Cargo.toml --all-targets -- -D warnings

# fuzz クレートの clippy だけを実行する
fuzz-clippy:
	cargo clippy --manifest-path fuzz/Cargo.toml --all-targets -- -D warnings

# fuzz クレートを cargo fmt で整形する
fuzz-fmt:
	cargo fmt --manifest-path fuzz/Cargo.toml

# ビルド成果物を削除する
clean:
	cargo clean
