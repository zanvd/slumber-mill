#!/usr/bin/env bash
# The full-stack soak; the README's "Full-stack soak" section describes what it does and checks.
#
#   SLUMBER_MILL_SOAK_DURATION       how long the driver uploads, a Go duration (default 10m)
#   SLUMBER_MILL_SOAK_DRAIN_TIMEOUT  how long it then waits for everything to finish (default 15m)
#   SLUMBER_MILL_SOAK_CHAOS_EVERY    seconds between injected failures (default 45)
set -euo pipefail
((BASH_VERSINFO[0] >= 5)) || {
  echo "soak.sh needs bash 5 or later" >&2
  exit 1
}

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
# Compose's own default, the checkout's directory name lower-cased and cleaned, keeps each checkout's soak apart:
# prepare takes the project down with its volumes before it starts.
base=${root##*/}
base=${base,,}
base=${base//[^a-z0-9_-]/}
[[ -n ${COMPOSE_PROJECT_NAME:-$base} ]] || {
  echo "soak.sh: the directory name leaves nothing for a project name; set COMPOSE_PROJECT_NAME" >&2
  exit 1
}
project=${COMPOSE_PROJECT_NAME:-$base}-soak
work=$root/var/out/soak
data=$work/data
tenants=$work/tenants
export SLUMBER_MILL_SOAK_DURATION=${SLUMBER_MILL_SOAK_DURATION:-10m}
export SLUMBER_MILL_SOAK_DRAIN_TIMEOUT=${SLUMBER_MILL_SOAK_DRAIN_TIMEOUT:-15m}
chaos_every=${SLUMBER_MILL_SOAK_CHAOS_EVERY:-45}

# The tenants: slug, target loudness, and whether webhooks go to the driver's receiver. Targets above -16 LUFS are
# left out: pink noise can't reach them under the -4.5 dBTP pre-encode peak, so loudnorm falls back to dynamic
# mode, undershoots by more than 1 LU, and the encode pool rejects the renditions.
slugs=(soak-a soak-b soak-c soak-d soak-quiet)
targets=(-16 -18 -20 -23 -16)
hooked=(yes yes yes yes no)

# The WARN line the soak's own traffic explains: the losing insert of two racing uploads with one idempotency key,
# which then replays the winner. Its count may not exceed the races the driver could have caused, which it writes to
# duplicate-insert-bound. Any ERROR line fails the soak.
duplicate_key='^api-[0-9]+ +\| .*"level":"WARN","msg":"query failed","method":"POST","route":"/v1/assets",.*"error":"duplicated key not allowed","sql":"INSERT INTO `assets` '

failures=0
results=()
follower=

compose() {
  docker compose --project-directory "$root" -p "$project" --progress quiet \
    -f "$root/compose.yaml" -f "$root/scripts/soak.compose.yaml" "$@"
}

log() {
  printf '%(%H:%M:%S)T %s\n' -1 "$*" >&2
}

# wait_for WHAT TIMEOUT_SECONDS COMMAND... polls COMMAND until it succeeds or the timeout passes.
wait_for() {
  local what=$1 timeout=$2
  shift 2
  local deadline=$((EPOCHSECONDS + timeout))
  until "$@"; do
    if ((EPOCHSECONDS >= deadline)); then
      log "timed out after ${timeout}s waiting for $what"
      return 1
    fi
    sleep 1
  done
}

# record PASS|FAIL DESCRIPTION
record() {
  results+=("$1  $2")
  [[ $1 == PASS ]] || failures=$((failures + 1))
}

# seconds DURATION converts a Go duration of hours, minutes and seconds to whole seconds, rounding up.
seconds() {
  local d=$1 total=0 n re='^([0-9]+)(\.[0-9]+)?(h|m|s)(.*)$'
  while [[ -n $d ]]; do
    if [[ ! $d =~ $re ]]; then
      log "can't read the duration $1"
      return 1
    fi
    n=${BASH_REMATCH[1]}
    case ${BASH_REMATCH[3]} in
      h) n=$((n * 3600)) ;;
      m) n=$((n * 60)) ;;
      s) [[ -n ${BASH_REMATCH[2]} ]] && n=$((n + 1)) ;;
    esac
    total=$((total + n))
    d=${BASH_REMATCH[4]}
  done
  echo "$total"
}

# logged SERVICE TEXT [COUNT] reports whether SERVICE's containers have logged TEXT at least COUNT times.
logged() {
  local n
  n=$(compose --profile soak logs --no-color --no-log-prefix "$1" 2>/dev/null | grep -cF -- "$2" || true)
  ((n >= ${3:-1}))
}

sql() {
  compose exec -T -e MYSQL_PWD=slumber_mill mysql mysql -uslumber_mill -N -B slumber_mill -e "$1"
}

# containers SERVICE prints the names of SERVICE's containers in replica order.
containers() {
  compose ps -a --format '{{.Name}}' "$1" | sort -V
}

state() {
  docker inspect -f '{{.State.Status}} {{.State.ExitCode}}' "$1"
}

name() {
  docker inspect -f '{{.Name}}' "$1" | sed 's,^/,,'
}

teardown() {
  local code=$?
  trap '' INT TERM
  [[ -n $follower ]] && kill "$follower" 2>/dev/null
  log "saving the logs to $work and taking the project down"
  compose logs --no-color --timestamps >"$work/compose.log" 2>&1 || true
  compose --profile soak logs --no-color --no-log-prefix soak >"$work/driver.log" 2>&1 || true
  compose --profile soak down -v --remove-orphans --rmi local >/dev/null 2>&1 ||
    log "error taking the project down; run: docker compose -p $project --profile soak down -v --remove-orphans --rmi local"
  rm -rf "${data:?}" "${tenants:?}"
  exit "$code"
}

prepare() {
  log "starting from a clean project $project"
  compose --profile soak down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "${data:?}" "${tenants:?}"
  rm -f "$work/summary.txt" "$work/compose.log" "$work/driver.log" "$work/tenant-create.log" \
    "$work/duplicate-insert-bound"
  mkdir -p "$data" "$tenants"

  log "building and starting the stack: api, two encode and two webhook workers"
  compose up -d --build
  wait_for "the API" 300 logged api "serving HTTP"
  wait_for "both encode workers" 300 logged worker-encode "worker pool started" 2
  wait_for "both webhook workers" 300 logged worker-webhook "worker pool started" 2

  log "building and starting the soak driver, which runs for $SLUMBER_MILL_SOAK_DURATION and then drains"
  compose --profile soak up -d --build --no-deps soak
  driver=$(compose --profile soak ps -q soak)
  wait_for "the driver's receiver" 900 logged soak "soak: receiver listening"

  local i args
  for i in "${!slugs[@]}"; do
    args=(-slug "${slugs[i]}" -target-lufs "${targets[i]}")
    [[ ${hooked[i]} == yes ]] && args+=(-webhook-url "http://soak:8090/hooks/${slugs[i]}")
    log "creating tenant ${slugs[i]}"
    compose run --rm --no-deps -T api tenant create "${args[@]}" \
      >"$tenants/${slugs[i]}.creds" 2>>"$work/tenant-create.log"
  done
  touch "$tenants/ready"
  wait_for "the driver to start uploading" 600 logged soak "soak: uploading"
}

# crash SERVICE N sends SIGKILL to the Nth container of SERVICE and starts it again a few seconds later. Its work in
# flight stays leased until the other replica's reaper requeues it, and a webhook worker holding the relay lock loses
# it with its session.
crash() {
  local cid st
  cid=$(containers "$1" | sed -n "${2}p")
  log "chaos: SIGKILL $(name "$cid")"
  docker kill -s KILL "$cid" >/dev/null
  wait_for "$(name "$cid") to die" 30 dead "$cid" || true
  st=$(state "$cid")
  check_exit "SIGKILL of $(name "$cid")" "$st" 137
  sleep $((5 + RANDOM % 15))
  docker start "$cid" >/dev/null
}

# stop SERVICE N sends SIGTERM to the Nth container of SERVICE, waits for it to drain and exit, and starts it again.
stop() {
  local cid st start
  cid=$(containers "$1" | sed -n "${2}p")
  log "chaos: SIGTERM $(name "$cid")"
  start=$EPOCHSECONDS
  docker stop "$cid" >/dev/null
  st=$(state "$cid")
  check_exit "SIGTERM of $(name "$cid") (stopped in $((EPOCHSECONDS - start))s)" "$st" 0
  docker start "$cid" >/dev/null
}

dead() {
  [[ $(docker inspect -f '{{.State.Running}}' "$1") == false ]]
}

declare -A exits=()
# check_exit WHAT "STATUS CODE" WANT_CODE records whether a container stopped by the soak exited with WANT_CODE.
check_exit() {
  local status code
  read -r status code <<<"$2"
  if [[ $status == exited && $code == "$3" ]]; then
    exits[ok]=$((${exits[ok]:-0} + 1))
  else
    exits[bad]=$((${exits[bad]:-0} + 1))
    log "$1: the container is $status with code $code, want exited with $3"
  fi
}

# chaos injects a failure every chaos_every seconds until the driver's uploads have run for their duration.
chaos() {
  local until=$(($1 + $(seconds "$SLUMBER_MILL_SOAK_DURATION"))) actions i=0
  actions=("crash worker-encode 1" "crash worker-webhook 1" "stop api 1" "crash worker-encode 2"
    "stop worker-webhook 2" "crash worker-webhook 2" "stop worker-webhook 1")
  RANDOM=1
  while ((EPOCHSECONDS + chaos_every < until)); do
    sleep "$chaos_every"
    if dead "$driver"; then
      log "the driver exited during the uploads"
      return
    fi
    ${actions[i % ${#actions[@]}]}
    i=$((i + 1))
    chaos_count=$i
  done
  log "chaos: $i failures injected"
}

# not_running prints the service containers that aren't running.
not_running() {
  local svc cid
  for svc in api worker-encode worker-webhook; do
    for cid in $(containers "$svc"); do
      dead "$cid" && echo "$(name "$cid") ($(state "$cid"))"
    done
  done
  return 0
}

# check_logs fails the soak on deadlocks, lock wait timeouts and ERROR lines in the services' logs,
# and counts what the pools logged.
check_logs() {
  local file=$work/services.log line svc json msg re n bound
  compose logs --no-color api worker-encode worker-webhook migrate >"$file" 2>&1
  declare -A errors=() outcomes=() reaped=()
  local deadlocks=0 lockwaits=0 relay_locks=0 swept=0 duplicates=0 error_lines=0
  re='^([a-z-]+-[0-9]+) +\| (.*)$'
  while IFS= read -r line; do
    [[ $line =~ $re ]] || continue
    svc=${BASH_REMATCH[1]}
    json=${BASH_REMATCH[2]}
    [[ $json == *"Error 1213"* || $json == *"Deadlock found"* ]] && deadlocks=$((deadlocks + 1))
    [[ $json == *"Error 1205"* || $json == *"Lock wait timeout"* ]] && lockwaits=$((lockwaits + 1))
    msg=
    [[ $json =~ \"msg\":\"([^\"]*)\" ]] && msg=${BASH_REMATCH[1]}
    case $msg in
      "work finished")
        [[ $json =~ \"pool\":\"([a-z]+)\" ]] && n=${BASH_REMATCH[1]}
        [[ $json =~ \"outcome\":\"([a-z]+)\" ]] && outcomes["$n ${BASH_REMATCH[1]}"]=$((${outcomes["$n ${BASH_REMATCH[1]}"]:-0} + 1))
        ;;
      "reaped assets whose lease expired" | "reaped deliveries whose lease expired")
        [[ $json =~ \"requeued\":([0-9]+) ]] && reaped["$msg, requeued"]=$((${reaped["$msg, requeued"]:-0} + BASH_REMATCH[1]))
        [[ $json =~ \"failed\":([0-9]+) ]] && reaped["$msg, failed"]=$((${reaped["$msg, failed"]:-0} + BASH_REMATCH[1]))
        ;;
      "took the relay lock") relay_locks=$((relay_locks + 1)) ;;
      "swept temp files") [[ $json =~ \"removed\":([0-9]+) ]] && swept=$((swept + BASH_REMATCH[1])) ;;
    esac
    if [[ $json == *'"level":"ERROR"'* ]]; then
      error_lines=$((error_lines + 1))
      errors["${svc%-[0-9]*}: $msg"]=$((${errors["${svc%-[0-9]*}: $msg"]:-0} + 1))
      echo "ERROR: $line" >>"$work/errors.log"
    elif [[ $json == *'"level":"WARN"'* && $line =~ $duplicate_key ]]; then
      duplicates=$((duplicates + 1))
    fi
  done <"$file"

  echo "logs:"
  for n in "${!outcomes[@]}"; do echo "  work finished, $n: ${outcomes[$n]}"; done | sort
  for n in "${!reaped[@]}"; do echo "  $n: ${reaped[$n]}"; done | sort
  echo "  relay lock taken: $relay_locks times; temp files swept at startup: $swept"
  echo "  deadlocks: $deadlocks; lock wait timeouts: $lockwaits"
  bound=$(cat "$work/duplicate-insert-bound" 2>/dev/null) || bound=
  echo "  duplicate-key WARN lines from the losing insert of racing idempotent uploads: $duplicates, of at most ${bound:-unknown}"
  echo "  ERROR lines: $error_lines"
  for n in "${!errors[@]}"; do echo "    $n: ${errors[$n]}"; done | sort

  # The soak never stops an encode worker gracefully and no encode fails but by rejection, so an attempt beyond an
  # asset's first can only be a reaper's requeue after a SIGKILL. A third reap fails the asset instead. Every run
  # that injects a failure SIGKILLs an encode worker first, so a run without a requeue proves nothing.
  local requeued=${reaped["reaped assets whose lease expired, requeued"]:-0}
  local what="every encode attempt beyond an asset's first followed a reap ($extra_attempts in the database, $requeued requeued)"
  if ((requeued > 0)) && [[ $extra_attempts == "$requeued" ]]; then
    record PASS "$what"
  else
    record FAIL "$what"
  fi
  what="the duplicate-key WARN lines number no more than the uploads that could race ($duplicates, at most ${bound:-unknown})"
  if [[ $bound =~ ^[0-9]+$ ]] && ((duplicates <= bound)); then
    record PASS "$what"
  else
    record FAIL "$what"
  fi
  if ((deadlocks == 0 && lockwaits == 0)); then
    record PASS "no deadlocks or lock wait timeouts in any service's logs"
  else
    record FAIL "no deadlocks or lock wait timeouts in any service's logs ($deadlocks deadlocks, $lockwaits lock waits)"
  fi
  if ((${#errors[@]} == 0)); then
    record PASS "no ERROR lines in any service's logs"
  else
    record FAIL "no ERROR lines in any service's logs (see $work/errors.log)"
  fi
}

main() {
  mkdir -p "$work"
  rm -f "$work/errors.log" "$work/services.log"
  trap teardown EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM
  chaos_count=0
  prepare

  local started=$EPOCHSECONDS code stopped
  compose --profile soak logs --follow --no-log-prefix --since 1s soak 2>/dev/null | grep --line-buffered 'soak: ' &
  follower=$!
  chaos "$started"

  stopped=$(not_running)
  if [[ -n $stopped ]]; then
    log "starting containers that are down: $stopped"
  fi
  for cid in $(containers api) $(containers worker-encode) $(containers worker-webhook); do
    dead "$cid" && docker start "$cid" >/dev/null
  done

  log "waiting for the driver to finish uploading, draining and checking"
  code=$(docker wait "$driver")
  kill "$follower" 2>/dev/null || true
  follower=
  log "the driver exited with code $code"
  extra_attempts=$(sql "SELECT COALESCE(SUM(attempts - 1), 0) FROM assets") || extra_attempts=unknown

  if [[ -z $stopped && -z $(not_running) ]]; then
    record PASS "no service container exited on its own"
  else
    record FAIL "no service container exited on its own: down after the uploads: ${stopped:-none}; at the end: $(not_running)"
  fi
  if ((chaos_count > 0 && ${exits[bad]:-0} == 0)); then
    record PASS "$chaos_count injected failures, each container exiting as expected: 0 after SIGTERM, 137 after SIGKILL"
  else
    record FAIL "$chaos_count injected failures, ${exits[bad]:-0} with the wrong exit status"
  fi
  if [[ $code == 0 ]]; then
    record PASS "the driver's invariants"
  else
    record FAIL "the driver's invariants (exit code $code; see $work/driver.log)"
  fi

  echo
  echo "full-stack soak, project $project"
  if [[ -f $work/summary.txt ]]; then
    cat "$work/summary.txt"
  else
    compose --profile soak logs --no-color --no-log-prefix --tail 60 soak
  fi
  check_logs
  echo
  printf '%s\n' "${results[@]}"
  echo "$((${#results[@]} - failures)) of ${#results[@]} checks passed in ${SECONDS}s"
  ((failures == 0))
}

main "$@"
