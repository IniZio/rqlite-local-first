#!/usr/bin/env bash
# Scripted walkthrough of the demo against a running `docker compose up`.
# Each step asserts what a person would see in the three browser tabs.
set -euo pipefail
cd "$(dirname "$0")/.."

declare -A port=([a]=8081 [b]=8082 [c]=8083)
fail() { echo "FAIL: $*" >&2; exit 1; }
step() { printf '\n== %s\n' "$*"; }

state() { curl -fsS "localhost:${port[$1]}/api/state"; }
add() { curl -fsS -X POST "localhost:${port[$1]}/api/items" -H 'Content-Type: application/json' -d "{\"text\":\"$2\"}"; }
show() {
  state "$1" | jq -r --arg d "$1" '"device \($d): online=\(.data.online) read=\(.data.read_level) raft=\(.cluster.raft_state) pending=\(.data.pending) items=\([.data.items[] | .text + (if .pending then "*" else "" end)] | join(", "))"'
}
has_item() { state "$1" | jq -e --arg t "$2" '.data.items | any(.text == $t and (.pending | not))' >/dev/null; }

# wait_for <seconds> <description> <command...>
wait_for() {
  local secs=$1 what=$2; shift 2
  for _ in $(seq "$((secs * 2))"); do "$@" && return 0; sleep 0.5; done
  fail "timed out after ${secs}s waiting for: $what"
}
online() { state "$1" | jq -e '.data.online' >/dev/null; }
offline() { state "$1" | jq -e '.data.online | not' >/dev/null; }
all_have() { has_item a "$1" && has_item b "$1" && has_item c "$1"; }

run_id=$(date +%H%M%S)

step "1. cluster up, every device online"
for d in a b c; do wait_for 60 "device $d online" online $d; show $d; done

step "2. write on A, read on B and C"
add a "milk-$run_id" | jq -c .
wait_for 5 "milk on every device" all_have "milk-$run_id"
for d in a b c; do show $d; done

step "3. stop node 2: 2 of 3 still form a quorum"
docker compose stop rqlite-2
wait_for 15 "A online with node 2 down" online a
add c "eggs-$run_id" | jq -c .
wait_for 5 "eggs on A" has_item a "eggs-$run_id"
show a; show c

step "4. restart node 2: it catches up from the Raft log"
docker compose start rqlite-2
wait_for 30 "B back online" online b
wait_for 10 "eggs on B" has_item b "eggs-$run_id"
show b

step "5. isolate device C from the cluster"
scripts/net.sh isolate c
wait_for 15 "C offline" offline c
add a "bread-$run_id" | jq -c .
r=$(add c "coffee-$run_id"); echo "$r"
[[ $(jq -r .status <<<"$r") == queued ]] || fail "C write should queue while isolated"
show a; show c
has_item c "bread-$run_id" && fail "C should not see bread while isolated"
state a | jq -e --arg t "coffee-$run_id" '.data.items | any(.text == $t) | not' >/dev/null || fail "A should not see C's queued write"

step "6. heal C: outbox drains, both sides converge"
scripts/net.sh heal c
wait_for 30 "C online" online c
wait_for 15 "coffee on every device" all_have "coffee-$run_id"
wait_for 5 "bread on C" has_item c "bread-$run_id"
for d in a b c; do show $d; done

step "7. lose quorum: stop nodes 2 and 3, A alone cannot commit"
docker compose stop rqlite-2 rqlite-3
wait_for 15 "A offline" offline a
r=$(add a "tea-$run_id"); echo "$r"
[[ $(jq -r .status <<<"$r") == queued ]] || fail "A write should queue without quorum"
show a

step "8. restore nodes 2 and 3: quorum returns, A drains"
docker compose start rqlite-2 rqlite-3
wait_for 60 "tea on every device" all_have "tea-$run_id"
for d in a b c; do show $d; done

printf '\nPASS: all steps\n'
