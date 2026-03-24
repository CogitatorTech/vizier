{
  description = "Vizier: A database advisor and finetuner for DuckDB";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = { self, nixpkgs }:
    let
      supportedSystems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
      forAllSystems = nixpkgs.lib.genAttrs supportedSystems;
    in
    {
      devShells = forAllSystems (system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
        in
        {
          default = pkgs.mkShell {
            packages = with pkgs; [
              # Build
              zig
              gnumake

              # Formatting
              clang-tools

              # Testing and development
              duckdb

              # Documentation
              (python3.withPackages (ps: with ps; [ mkdocs ]))
              uv

              # Git hooks
              pre-commit
            ];
          };
        }
      );

      packages = forAllSystems (system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
        in
        {
          default = pkgs.stdenv.mkDerivation {
            name = "vizier";
            src = ./.;

            nativeBuildInputs = [ pkgs.zig pkgs.gnumake ];

            buildPhase = ''
              export ZIG_GLOBAL_CACHE_DIR=$(mktemp -d)
              make build-all
            '';

            installPhase = ''
              mkdir -p $out/lib
              find zig-out/lib -name "*.duckdb_extension" -exec cp {} $out/lib/ \;
            '';
          };
        }
      );
    };
}
