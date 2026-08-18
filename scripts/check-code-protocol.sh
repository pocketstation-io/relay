#!/usr/bin/env bash
set -euo pipefail

repository_root=$(cd "$(dirname "$0")/.." && pwd)
cd "$repository_root"

if unformatted=$(gofmt -l cmd internal test); [[ -n "$unformatted" ]]; then
  printf 'CODE_PROTOCOL: gofmt violations:\n%s\n' "$unformatted" >&2
  exit 1
fi

if vague_directories=$(find cmd internal test -type d \( \
  -name utils -o -name helpers -o -name common -o -name misc -o \
  -name shared -o -name base -o -name core -o -name manager -o \
  -name handler -o -name system \
\)); [[ -n "$vague_directories" ]]; then
  printf 'CODE_PROTOCOL: vague ownership directories:\n%s\n' "$vague_directories" >&2
  exit 1
fi

if legacy_wrappers=$(find internal -type d \( \
  -path internal/access -o -path internal/admission/ratelimit -o \
  -path internal/protocol -o -path internal/observability -o \
  -path internal/traversal \
\)); [[ -n "$legacy_wrappers" ]]; then
  printf 'CODE_PROTOCOL: false ownership wrappers:\n%s\n' "$legacy_wrappers" >&2
  exit 1
fi

if [[ -e cmd/fake-source ]]; then
  printf 'CODE_PROTOCOL: ambiguous fake-source command remains\n' >&2
  exit 1
fi

if section_banners=$(rg --no-ignore -n '^\s*//\s*[-=*#]{3,}' cmd internal test --glob '*.go' || true); [[ -n "$section_banners" ]]; then
  printf 'CODE_PROTOCOL: section divider comments:\n%s\n' "$section_banners" >&2
  exit 1
fi

test_functions=$(rg --no-ignore -n '^func Test[A-Za-z0-9_]+' cmd internal test --glob '*.go' || true)
if bad_tests=$(printf '%s\n' "$test_functions" | grep -v -E 'func TestGiven.+When.+Then|func TestMain' || true); [[ -n "$bad_tests" ]]; then
  printf 'CODE_PROTOCOL: tests without TestGiven...When...Then names:\n%s\n' "$bad_tests" >&2
  exit 1
fi

if semantic_booleans=$(rg --no-ignore -n --pcre2 \
  '\b(reason|cause|category|policy|mode|direction|role|strategy|ownership|outcome|overrun)\s*:?\s*bool\b' \
  cmd internal test --glob '*.go' || true); [[ -n "$semantic_booleans" ]]; then
  printf 'CODE_PROTOCOL: semantic choices encoded as booleans:\n%s\n' "$semantic_booleans" >&2
  exit 1
fi

if rg --no-ignore -n 'internal/graph|\bgraph\.' cmd internal test --glob '*.go'; then
  printf 'CODE_PROTOCOL: legacy graph ownership remains\n' >&2
  exit 1
fi

if rg --no-ignore -n 'internal/media/downlink' internal/session --glob '*.go'; then
  printf 'CODE_PROTOCOL: session depends on the concrete downlink transport\n' >&2
  exit 1
fi

if rg --no-ignore -n 'slog\.|log\.|fmt\.Print' internal/session --glob '*.go'; then
  printf 'CODE_PROTOCOL: session forwarding ownership must remain log-free\n' >&2
  exit 1
fi

if rg --no-ignore -n '\.\./\.\./soak/results|soak/results' test/soak --glob '*.go'; then
  printf 'CODE_PROTOCOL: tests write to tracked soak artifacts\n' >&2
  exit 1
fi

go vet ./...
go test -short ./...
