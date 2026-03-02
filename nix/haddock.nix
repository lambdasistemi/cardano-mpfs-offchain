# Haddock documentation for gh-pages deployment.
#
# Copies haddock output, rewrites nix-store file:// links to
# relative URLs, and generates an index.
#
# Usage:
#   nix build .#haddock
#   ls result/          # cardano-mpfs-offchain/ index.html
{ pkgs, project }:

let
  haddock =
    pkgs.buildPackages.haskell-nix.compiler.ghc984.passthru.haddock or pkgs.haskellPackages.haddock;

  offchainDoc = project.hsPkgs.cardano-mpfs-offchain.components.library.doc;

  offchainName = "cardano-mpfs-offchain";

  offchainHtml = "${offchainDoc}/share/doc/${offchainName}/html";

in pkgs.runCommand "combined-haddock" {
  nativeBuildInputs = [ pkgs.haskell-nix.compiler.ghc984 ];
} ''
  mkdir -p $out

  cp -R ${offchainHtml} $out/${offchainName}
  chmod -R +w $out

  # Rewrite file:// store-path references to relative URLs
  for f in $(find $out -name "*.html" -o -name "*.json"); do
    dir=$(dirname "$f")
    rel_offchain=$(realpath --relative-to="$dir" "$out/${offchainName}")

    sed -i \
      -e "s|file://${offchainHtml}|$rel_offchain|g" \
      "$f"
    # Rewrite GHC boot-library file:// refs to Hackage URLs
    ${pkgs.perl}/bin/perl -pi -e \
      's{file:///nix/store/[^/]+-ghc-[\d.]+-doc/share/doc/ghc-[\d.]+/html/libraries/([^/]+)-inplace/}{https://hackage.haskell.org/package/$1/docs/}g' \
      "$f"
  done

  # Generate index and contents page
  haddock \
    --gen-contents \
    --gen-index \
    --odir=$out \
    --read-interface=${offchainName},${offchainHtml}/${offchainName}.haddock \
    || true

  # Copy shared static assets
  for asset in linuwial.css quick-jump.css quick-jump.min.js \
               haddock-bundle.min.js; do
    if [ -f ${offchainHtml}/$asset ] && [ ! -f $out/$asset ]; then
      cp ${offchainHtml}/$asset $out/
    fi
  done
''
