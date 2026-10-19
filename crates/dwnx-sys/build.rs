//! dwnx のソースを取得してビルドし、バインディングを生成する
//!
//! - `Cargo.toml` の `[package.metadata.external-dependencies]` に宣言した
//!   git リポジトリを `git clone` し、指定した ref をチェックアウトする
//!   (git submodule は使わない)
//! - configure が生成する `dwnx/version.h` を `version.h.in` から生成する
//! - `lib/*.c` を `cc` で静的ライブラリとしてコンパイルする
//! - `bindgen` で `dwnx.h` から FFI 宣言を生成する
//!
//! git と libclang (bindgen 用)、C11 コンパイラが必要である。

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

/// `[package.metadata.external-dependencies]` に宣言した取得先
struct DwnxSource {
    git: String,
    reference: String,
}

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR"));

    // ref を変えたら再ビルドする
    println!("cargo:rerun-if-changed=Cargo.toml");

    let source = read_source(&manifest_dir);
    let dwnx_dir = checkout(&out_dir, &source);

    // configure が生成する version.h を再現する。dwnx.h がこれを include する。
    let version_in = fs::read_to_string(dwnx_dir.join("lib/includes/dwnx/version.h.in"))
        .expect("version.h.in is readable");
    // 固定した ref の AC_INIT([dwnx], [0.0.0-DEV]) に対応する値
    let version = version_in
        .replace("@PACKAGE_VERSION@", "0.0.0-DEV")
        .replace("@PACKAGE_VERSION_NUM@", "0x000000");
    let generated_include = out_dir.join("include");
    let generated_version_dir = generated_include.join("dwnx");
    fs::create_dir_all(&generated_version_dir).expect("generated include directory is created");
    fs::write(generated_version_dir.join("version.h"), version).expect("version.h is written");

    // libdwnx を静的ライブラリとしてビルドする
    let mut build = cc::Build::new();
    build.include(dwnx_dir.join("lib"));
    build.include(dwnx_dir.join("lib/includes"));
    build.include(&generated_include);
    build.define("DWNX_STATICLIB", None);
    build.warnings(false);

    // configure が生成する config.h 相当のヘッダ有無マクロを定義する。
    // dwnx は HAVE_ARPA_INET_H などが未定義だと <arpa/inet.h> を取り込まず、
    // ntohl / htons などの宣言が無いままになる。新しいコンパイラは暗黙宣言を
    // エラーにするため、configure と同じマクロをここで定義する。
    // HAVE_DECL_BE64TOH / HAVE_DECL_BSWAP_64 は未定義のままにして、
    // ntohl を使う可搬なフォールバックを選ぶ。
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    // Windows は未対応であり、この分岐は CI でビルドしていない (未検証)。
    // Windows には <arpa/inet.h> などが無いため、ヘッダ有無マクロを定義しない。
    // Windows へ対応する場合は、必要なヘッダと ntohl などの代替実装を確認すること。
    if target_os != "windows" {
        build.define("HAVE_ARPA_INET_H", None);
        build.define("HAVE_NETINET_IN_H", None);
        build.define("HAVE_UNISTD_H", None);
    }
    for entry in fs::read_dir(dwnx_dir.join("lib")).expect("lib directory is readable") {
        let path = entry.expect("directory entry is readable").path();
        if path.extension().is_some_and(|extension| extension == "c") {
            build.file(path);
        }
    }
    build.compile("dwnx");

    // FFI 宣言を生成する
    let bindings = bindgen::Builder::default()
        .header(
            dwnx_dir
                .join("lib/includes/dwnx/dwnx.h")
                .to_string_lossy()
                .into_owned(),
        )
        .clang_arg(format!("-I{}", dwnx_dir.join("lib/includes").display()))
        .clang_arg(format!("-I{}", generated_include.display()))
        .clang_arg("-DDWNX_STATICLIB")
        .allowlist_function("dwnx_.*")
        .allowlist_type("dwnx_.*")
        .allowlist_var("DWNX_.*")
        .generate()
        .expect("dwnx bindings are generated");
    bindings
        .write_to_file(out_dir.join("bindings.rs"))
        .expect("bindings.rs is written");
}

/// `Cargo.toml` の `[package.metadata.external-dependencies]` を読む
fn read_source(manifest_dir: &Path) -> DwnxSource {
    let manifest =
        fs::read_to_string(manifest_dir.join("Cargo.toml")).expect("Cargo.toml is readable");
    let toml = shiguredo_toml::from_str(&manifest).expect("Cargo.toml is parsed");
    let deps = toml["package"]["metadata"]["external-dependencies"]
        .as_table()
        .expect("[package.metadata.external-dependencies] is required");
    let dwnx = deps["dwnx"].as_table().expect("dwnx entry is required");
    let git = dwnx["git"].as_str().expect("git is required").to_string();
    let reference = dwnx["ref"].as_str().expect("ref is required").to_string();
    DwnxSource { git, reference }
}

/// dwnx を clone し、指定した ref をチェックアウトしてパスを返す
///
/// チェックアウト先はビルドプロファイルごとの target ディレクトリに置き、
/// 同じ ref であれば再 clone しない。
fn checkout(out_dir: &Path, source: &DwnxSource) -> PathBuf {
    // OUT_DIR = <target>/<profile>/build/<package>/out
    let target_dir = out_dir
        .ancestors()
        .nth(3)
        .expect("target directory is found")
        .to_path_buf();
    let checkout_dir = target_dir.join(format!("dwnx-{}", source.reference));

    if !checkout_dir.join(".git").is_dir() {
        let _ = fs::remove_dir_all(&checkout_dir);
        run(Command::new("git")
            .arg("clone")
            .arg("--quiet")
            .arg(&source.git)
            .arg(&checkout_dir));
    }
    run(Command::new("git")
        .arg("-C")
        .arg(&checkout_dir)
        .arg("checkout")
        .arg("--quiet")
        .arg(&source.reference));

    checkout_dir
}

/// コマンドを実行し、失敗したら panic する
fn run(command: &mut Command) {
    let status = command
        .status()
        .unwrap_or_else(|error| panic!("failed to run {command:?}: {error}"));
    assert!(status.success(), "{command:?} failed: {status}");
}
