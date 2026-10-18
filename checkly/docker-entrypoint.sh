#!/bin/sh
set -eu

stamp=node_modules/.lock-sha256
want=$(sha256sum package-lock.json | cut -d ' ' -f 1)

if [ ! -f "$stamp" ] || [ "$(cat "$stamp")" != "$want" ]; then
  npm ci >&2
  echo "$want" > "$stamp"
fi

exec "$@"
