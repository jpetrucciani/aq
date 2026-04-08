{ pkgs ? import
    (fetchTarball {
      name = "jpetrucciani-2026-04-01";
      url = "https://github.com/jpetrucciani/nix/archive/9a3a8de1ca81970ebf64c773c3ca9aa0301d7c90.tar.gz";
      sha256 = "0gxfdzrwz9bb42byinwsjhmi72yifimhypjbv9v9g6haxza46l5y";
    })
    { overlays = [ rustOverlay ]; }
, rustOverlay ? import
    (fetchTarball {
      name = "oxalica-2026-04-01";
      url = "https://github.com/oxalica/rust-overlay/archive/e8046c1d9ccadd497c2344d8fa49dab62f22f7be.tar.gz";
      sha256 = "0371f7g0sans86jbx4pkvcdmzn46fyk63mln7g8vzmjchgmfvync";
    })
}:
let
  name = "aq";
  muslTarget = "x86_64-unknown-linux-musl";

  rust = pkgs.rust-bin.selectLatestNightlyWith (toolchain: toolchain.default.override {
    extensions = [ "rust-src" "rustc-dev" "rust-analyzer" ];
    targets = [
      "x86_64-unknown-linux-musl"
      "aarch64-unknown-linux-musl"
    ];
  });

  rustPlatform = pkgs.makeRustPlatform {
    cargo = rust;
    rustc = rust;
  };

  scripts = with pkgs; {
    fmt = writers.writeBashBin "fmt" ''
      set -euo pipefail
      cargo fmt
    '';

    clippy_all = writers.writeBashBin "clippy_all" ''
      set -euo pipefail
      cargo clippy --all --benches --tests --examples --all-features -- -D warnings
    '';

    test_all_features = writers.writeBashBin "test_all_features" ''
      set -euo pipefail
      cargo test --all-features
    '';

    test_no_default_features = writers.writeBashBin "test_no_default_features" ''
      set -euo pipefail
      cargo test --no-default-features
    '';

    quality = writers.writeBashBin "quality" ''
      set -euo pipefail
      cargo fmt --check
      cargo clippy --all --benches --tests --examples --all-features -- -D warnings
      cargo test --all-features
      cargo test --no-default-features
    '';

    docs_build = writers.writeBashBin "docs_build" ''
      set -euo pipefail
      (
        cd docs
        bun install --frozen-lockfile
        bun run docs:build
      )
    '';

    jq_compat = writers.writeBashBin "jq_compat" ''
      set -euo pipefail
      python3 scripts/jq_compat_report.py
    '';

    jq_upstream_compat = writers.writeBashBin "jq_upstream_compat" ''
      set -euo pipefail
      python3 scripts/jq_upstream_report.py
    '';

    jq_upstream_benchmark = writers.writeBashBin "jq_upstream_benchmark" ''
      set -euo pipefail
      python3 scripts/jq_upstream_benchmark.py
    '';

    yq_upstream_benchmark = writers.writeBashBin "yq_upstream_benchmark" ''
      set -euo pipefail
      python3 scripts/yq_upstream_benchmark.py
    '';

    build_static = writers.writeBashBin "build_static" ''
      set -euo pipefail
      cargo zigbuild --release --locked --all-features --target ${muslTarget}
    '';
  };

  packages = with pkgs; [
    bun
    cargo-zigbuild
    jq
    jfmt
    pkg-config
    python314
    rust
    yq-go
    zig
  ] ++ builtins.attrValues scripts;

  shell = pkgs.mkShellNoCC {
    inherit name packages;
    RUST_SRC_PATH = "${rust}/lib/rustlib/src/rust/library";
  };

  bin = rustPlatform.buildRustPackage {
    pname = name;
    version = "0.0.0";
    src = pkgs.hax.filterSrc { path = ./.; };
    cargoLock.lockFile = ./Cargo.lock;
    auditable = false;
    strictDeps = true;
    nativeBuildInputs = with pkgs; [
      cargo-zigbuild
      pkg-config
      zig
    ];
    buildPhase = ''
      export HOME="$(mktemp -d)"
      cargo zigbuild --release --locked --all-features --target ${muslTarget}
    '';
    installPhase = ''
      mkdir -p "$out/bin"
      cp "target/${muslTarget}/release/${name}" "$out/bin/${name}"
    '';
    meta.mainProgram = name;
  };
in
(shell.overrideAttrs (_: { inherit name; })) // {
  inherit bin scripts;
}
