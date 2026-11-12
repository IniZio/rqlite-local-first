#!/usr/bin/env bash
set -euo pipefail

action=${1:?isolate|heal}
device=${2:?a|b|c}
case $device in a) n=1 ;; b) n=2 ;; c) n=3 ;; *) echo "unknown device $device" >&2; exit 2 ;; esac

project=rqlite-local-first
container=$(docker compose -p "$project" ps -q "rqlite-$n")
network="${project}_wan"

case $action in
  isolate) docker network disconnect "$network" "$container" ;;
  heal) docker network connect --alias "rqlite-$n" "$network" "$container" ;;
  *) echo "unknown action $action" >&2; exit 2 ;;
esac
echo "$action device $device (rqlite-$n)"
