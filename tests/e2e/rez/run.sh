#!/usr/bin/env bash
# Produce a known set of context tracking events with real rez commands.
# tests/e2e/test_rez_e2e.py checks that exactly these end up in Postgres.
set -euo pipefail

export SHOW=e2e_show

echo "1) resolve and run a command -> created"
rez-env foo -- python -c "print('inside rez-env')"

echo "2) resolve and save the context -> created"
rez-env foo --output /tmp/foo.rxt

echo "3) load the saved context -> sourced"
rez-env --input /tmp/foo.rxt -- python -c "print('inside sourced context')"

echo "4) conflicting request -> created, status failed"
if rez-env foo '!bar' -- true; then
    echo "expected the conflicting resolve to fail" >&2
    exit 1
fi

echo "done"
