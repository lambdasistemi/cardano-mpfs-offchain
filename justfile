# shellcheck shell=bash

set unstable := true

# List available recipes
default:
    @just --list

# Format all source files
format:
    nix run --quiet .#format

# Check formatting without modifying files
format-check:
    nix run --quiet .#format-check

# Run hlint
hlint:
    nix run --quiet .#hlint

# Build all components
build:
    #!/usr/bin/env bash
    set -euo pipefail
    cabal build all -O0 --enable-tests --enable-benchmarks

# Run unit tests with optional match pattern
unit match="":
    #!/usr/bin/env bash
    set -euo pipefail
    args=()
    if [[ '{{ match }}' != "" ]]; then
        args+=(--match "{{ match }}")
    fi
    nix run --quiet .#unit-tests -- "${args[@]}"

# Run offchain unit tests with optional match
unit-offchain match="":
    just unit "{{ match }}"

# Run client unit tests with optional match
unit-client match="":
    #!/usr/bin/env bash
    set -euo pipefail
    args=()
    if [[ '{{ match }}' != "" ]]; then
        args+=(--match "{{ match }}")
    fi
    nix run --quiet .#client-unit-tests -- "${args[@]}"

# Run workflows unit tests with optional match
unit-workflows match="":
    #!/usr/bin/env bash
    set -euo pipefail
    args=()
    if [[ '{{ match }}' != "" ]]; then
        args+=(--match "{{ match }}")
    fi
    nix run --quiet .#workflows-unit-tests -- "${args[@]}"

# Non-Docker CI gate (mirrors .github/workflows/ci.yml)
ci:
    nix build --quiet .#cardano-mpfs-offchain .#checks.x86_64-linux.swagger-up-to-date
    just unit
    just unit-client
    just unit-workflows
    just format-check
    just hlint

# Run E2E tests (starts cardano-node as subprocess)
e2e match="":
    #!/usr/bin/env bash
    set -euo pipefail
    args=()
    if [[ '{{ match }}' != "" ]]; then
        args+=(--match "{{ match }}")
    fi
    nix run --quiet .#e2e-tests -- "${args[@]}"

# Run the #278 facts API coverage matrix only.
# See specs/278-local-cluster-facts-api-coverage-matrix/quickstart.md.
e2e-facts-matrix:
    just e2e "facts API coverage matrix"

# Regenerate docs/assets/swagger.json
update-swagger:
    #!/usr/bin/env bash
    set -euo pipefail
    cabal -v0 run cardano-mpfs-swagger -O0 > docs/assets/swagger.json

# Serve documentation locally
docs:
    mkdocs serve

# Serve hoogle (all dependencies, from nix shell)
hoogle port="8080":
    #!/usr/bin/env bash
    set -euo pipefail
    echo "Serving hoogle on http://localhost:{{ port }}"
    echo "(includes all dependencies; local packages not yet indexed — see issue #59)"
    hoogle server --local --port={{ port }}

# Build docker image via Nix
build-docker tag='latest':
    #!/usr/bin/env bash
    set -euo pipefail
    nix build .#docker-image
    docker load < result
    version=$(nix eval --raw .#version)
    docker image tag \
        "ghcr.io/lambdasistemi/cardano-mpfs-offchain/mpfs-serve:$version" \
        "ghcr.io/lambdasistemi/cardano-mpfs-offchain/mpfs-serve:{{ tag }}"

# Clean build artifacts
clean:
    #!/usr/bin/env bash
    cabal clean
    rm -rf result
