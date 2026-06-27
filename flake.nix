{
    inputs = {
        nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
        flake-utils.url = "github:numtide/flake-utils";

        rust-overlay = {
            url = "github:oxalica/rust-overlay";
            inputs.nixpkgs.follows = "nixpkgs";
        };
    };

    outputs = { self, nixpkgs, flake-utils, rust-overlay }:
        flake-utils.lib.eachDefaultSystem (system:
            let
                pkgs = import nixpkgs {
                    inherit system;

                    overlays = [ rust-overlay.overlays.default ];
                };

                muslPkgs = pkgs.pkgsCross.musl64;
            in {
                devShells.default = pkgs.mkShell {
                    nativeBuildInputs = with pkgs; [
                        (pkgs.pkgs.rust-bin.stable.latest.default.override {
                            targets = [ "x86_64-unknown-linux-musl" ];
                            extensions = [ "rust-src" ];
                        })
                        muslPkgs.buildPackages.gcc
                        protobuf
                        lld
                    ];
                };

                packages.default = muslPkgs.rustPlatform.buildRustPackage {
                    pname = "sophon-tools";
                    version = "0.2.0";

                    src = ./.;
                    cargoLock.lockFile = ./Cargo.lock;

                    nativeBuildInputs = with pkgs; [
                        protobuf
                    ];

                    doCheck = false;

                    postInstall = ''
                        mv $out/bin/sophon-cli $out/bin/sophon-tools
                    '';
                };
            });
}
