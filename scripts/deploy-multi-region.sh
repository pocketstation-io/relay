#!/usr/bin/env bash
# Deploy PocketStation relay to all three regions.
# Requires: flyctl installed, FLY_API_TOKEN set, fly.toml present.
set -euo pipefail

fly deploy --remote-only
fly scale count 2 --region fra,iad,nrt
fly status
