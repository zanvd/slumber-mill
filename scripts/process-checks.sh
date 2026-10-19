#!/usr/bin/env bash
# The process-level checks: how worker-encode stops, drains and crashes under Compose.
#
# Runs on its own Compose project, named <COMPOSE_PROJECT_NAME or the checkout's directory name>-process-checks:
# brings it up, and takes it down with its volumes and images on exit. Needs only bash 5 and Docker; curl, mysql
# and ffmpeg run in containers. Generated files go to var/out/process-checks.
set -euo pipefail
((BASH_VERSINFO[0] >= 5)) || {
  echo "process-checks.sh needs bash 5 or later" >&2
  exit 1
}

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
# Compose's own default, the checkout's directory name lower-cased and cleaned, keeps each checkout's stack apart:
# the teardown removes the project's volumes.
base=${root##*/}
base=${base,,}
base=${base//[^a-z0-9_-]/}
[[ -n ${COMPOSE_PROJECT_NAME:-$base} ]] || {
  echo "process-checks.sh: the directory name leaves nothing for a project name; set COMPOSE_PROJECT_NAME" >&2
  exit 1
}
project=${COMPOSE_PROJECT_NAME:-$base}-process-checks
work=$root/var/out/process-checks
data=$work/data
fixture=$work/fixture.wav

# Stereo noise long enough that an encode outlasts the shutdown grace: about 90 s of encoding on one core.
fixture_seconds=600
# Docker's default stop timeout, which check 3 stops with.
short_stop=10
# How long past the lease check 2 waits before restarting, for clock skew between Docker's VM and the host.
lease_margin=10

failures=0
results=()
api_key=
asset_a=

# COMPOSE_PROFILES is emptied so that a checkout's .env opt-in to the Checkly agent doesn't start a second agent here.
compose() {
  COMPOSE_PROFILES= docker compose --project-directory "$root" -p "$project" --progress quiet \
    -f "$root/compose.yaml" -f "$root/scripts/process-checks.compose.yaml" "$@"
}

log() {
  printf '%(%H:%M:%S)T %s\n' -1 "$*" >&2
}

now_ms() {
  local us=${EPOCHREALTIME/[.,]/}
  echo $((us / 1000))
}

since() {
  local ms=$(($(now_ms) - $1))
  printf '%d.%01d' $((ms / 1000)) $((ms % 1000 / 100))
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
    sleep 0.5
  done
}

# expect DESCRIPTION COMMAND... reports whether COMMAND succeeds.
expect() {
  local what=$1
  shift
  if "$@"; then
    echo "    ok      $what"
  else
    echo "    FAILED  $what"
    return 1
  fi
}

run_check() {
  local name=$1
  shift
  log "check: $name"
  echo "check $name"
  if "$@"; then
    results+=("PASS  $name")
  else
    results+=("FAIL  $name")
    failures=$((failures + 1))
  fi
  echo "${results[-1]}"
}

sql() {
  compose exec -T -e MYSQL_PWD=slumber_mill mysql mysql -uslumber_mill -N -B slumber_mill -e "$1"
}

# asset_state ID prints the asset's status and attempts.
asset_state() {
  sql "SELECT status, attempts FROM assets WHERE public_id = UUID_TO_BIN('$1')"
}

# state_is ID STATUS [ATTEMPTS]
state_is() {
  local status attempts
  read -r status attempts < <(asset_state "$1") || return 1
  [[ $status == "$2" && (-z ${3:-} || $attempts == "$3") ]]
}

state_text() {
  local status attempts
  if read -r status attempts < <(asset_state "$1"); then
    echo "$status with attempts $attempts"
  else
    echo "unknown"
  fi
}

attempts_of() {
  local status attempts
  read -r status attempts < <(asset_state "$1") || return 1
  echo "$attempts"
}

none_processing() {
  [[ $(sql "SELECT COUNT(*) FROM assets WHERE status = 'processing'") == 0 ]]
}

http() {
  compose run --rm --no-deps -T curl "$@"
}

api_ready() {
  http -fsS -o /dev/null http://api:8080/readyz 2>/dev/null
}

# upload TITLE posts the fixture and prints the new asset's ID.
upload() {
  local body
  body=$(http -sS --fail-with-body -H "Authorization: Bearer $api_key" \
    -F "title=$1" -F "file=@/fixtures/${fixture##*/}" http://api:8080/v1/assets) || {
    log "upload failed: $body"
    return 1
  }
  if [[ ! $body =~ \"id\":\"([0-9a-f-]{36})\" ]]; then
    log "no asset ID in the upload's response: $body"
    return 1
  fi
  echo "${BASH_REMATCH[1]}"
}

encode_container() {
  compose ps -aq worker-encode
}

# start_encode restarts the stopped worker-encode container; compose start would run migrate again first.
start_encode() {
  docker start "$(encode_container)" >/dev/null
}

running() {
  [[ $(docker inspect -f '{{.State.Running}}' "$1") == true ]]
}

stopped() {
  ! running "$1"
}

exit_code() {
  docker inspect -f '{{.State.ExitCode}}' "$1"
}

# last_log_value MESSAGE FIELD prints FIELD from worker-encode's last log line with MESSAGE.
last_log_value() {
  local line value= re="\"$2\":\"?([^\",}]+)"
  while IFS= read -r line; do
    if [[ $line == *"\"msg\":\"$1\""* && $line =~ $re ]]; then
      value=${BASH_REMATCH[1]}
    fi
  done < <(compose logs --no-color --no-log-prefix worker-encode)
  [[ -n $value ]] && echo "$value"
}

pool_started() {
  last_log_value "worker pool started" lease >/dev/null
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

temp_files() {
  local f
  for f in "$data"/tmp/part-*; do
    [[ -e $f ]] && echo "$f"
  done
  return 0
}

temp_files_at_least() {
  local files
  mapfile -t files < <(temp_files)
  ((${#files[@]} >= $1))
}

teardown() {
  local code=$?
  trap '' INT TERM
  log "saving the logs to $work/compose.log and taking the project down"
  compose logs --no-color --timestamps >"$work/compose.log" 2>&1 || true
  compose --profile process-checks down -v --remove-orphans --rmi local >/dev/null 2>&1 ||
    log "error taking the project down; run: docker compose -p $project down -v --remove-orphans --rmi local"
  rm -rf "${data:?}"
  rm -f "$fixture"
  exit "$code"
}

prepare() {
  log "starting from a clean project $project"
  compose --profile process-checks down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "${data:?}"
  mkdir -p "$data"

  log "building and starting the stack"
  compose up -d --build
  wait_for "the API" 180 api_ready
  wait_for "the encode pool" 180 pool_started

  log "generating ${fixture_seconds}s of stereo pink noise"
  compose run --rm --no-deps -T -v "$work:/fixtures" --entrypoint ffmpeg worker-encode \
    -nostdin -hide_banner -loglevel error -y \
    -f lavfi -i "anoisesrc=color=pink:amplitude=0.5:seed=1:sample_rate=48000:duration=$fixture_seconds" \
    -f lavfi -i "anoisesrc=color=pink:amplitude=0.5:seed=2:sample_rate=48000:duration=$fixture_seconds" \
    -filter_complex '[0:a][1:a]amerge=inputs=2' -c:a pcm_s16le /fixtures/fixture.wav

  log "creating a tenant"
  local creds
  creds=$(compose run --rm --no-deps -T api tenant create -slug process-checks)
  if [[ ! $creds =~ api_key:\ ([^[:space:]]+) ]]; then
    log "tenant create printed no API key"
    return 1
  fi
  api_key=${BASH_REMATCH[1]}

  lease=$(seconds "$(last_log_value "worker pool started" lease)")
  grace=$(seconds "$(last_log_value "worker pool started" grace)")
  stop_grace=$(docker inspect -f '{{.Config.StopTimeout}}' "$(encode_container)")
  log "worker-encode: lease ${lease}s, shutdown grace ${grace}s, stop_grace_period ${stop_grace}s"
  if ((!(short_stop < grace && grace < stop_grace))); then
    log "the checks need ${short_stop}s < the shutdown grace (${grace}s) < stop_grace_period (${stop_grace}s)"
    return 1
  fi
}

# Check 1: compose stop mid-encode drains, releases the asset and exits 0 within stop_grace_period.
check_stop() {
  local cid attempts start took code ok=0
  asset_a=$(upload "Process checks: stop") || return 1
  wait_for "asset $asset_a to be encoding" 60 state_is "$asset_a" processing || return 1
  attempts=$(attempts_of "$asset_a") || return 1
  cid=$(encode_container) || return 1

  log "stopping worker-encode while asset $asset_a is encoding (attempt $attempts)"
  start=$(now_ms)
  compose stop worker-encode || return 1
  took=$(since "$start")
  code=$(exit_code "$cid") || return 1

  expect "worker-encode exited with code $code, want 0" test "$code" -eq 0 || ok=1
  expect "it stopped after ${took}s, within the ${stop_grace}s stop_grace_period" \
    test "${took%.*}" -lt "$stop_grace" || ok=1
  expect "asset is $(state_text "$asset_a"), want queued with attempts $((attempts - 1)): released" \
    state_is "$asset_a" queued $((attempts - 1)) || ok=1
  return "$ok"
}

# Check 4: a second SIGTERM during the drain ends worker-encode at once.
check_second_sigterm() {
  local cid start took ok=0
  start_encode || return 1
  wait_for "asset $asset_a to be encoding again" 60 state_is "$asset_a" processing || return 1
  cid=$(encode_container) || return 1

  log "sending SIGTERM to worker-encode while asset $asset_a is encoding"
  compose kill -s SIGTERM worker-encode || return 1
  sleep 3
  expect "worker-encode is still running 3s after the first SIGTERM: draining" running "$cid" || return 1

  log "sending the second SIGTERM"
  start=$(now_ms)
  compose kill -s SIGTERM worker-encode || return 1
  wait_for "worker-encode to exit" 30 stopped "$cid" || return 1
  took=$(since "$start")
  expect "it exited ${took}s after the second SIGTERM (code $(exit_code "$cid")), within 5s and the ${grace}s grace" \
    test "${took%.*}" -lt 5 || ok=1
  return "$ok"
}

# Check 2: SIGKILL mid-encode; after the lease has passed, a restart sweeps the dead attempt's temp files and
# reaps the asset, which is processed again as attempt 2.
check_sigkill() {
  local cid asset_b earlier dead f left=() swept ok=0
  mapfile -t earlier < <(temp_files)
  asset_b=$(upload "Process checks: kill") || return 1
  start_encode || return 1
  wait_for "asset $asset_b to be encoding" 60 state_is "$asset_b" processing 1 || return 1
  wait_for "asset $asset_b's temp outputs" 30 temp_files_at_least $((${#earlier[@]} + 2)) || return 1
  cid=$(encode_container) || return 1

  log "killing worker-encode while asset $asset_b is encoding"
  compose kill -s SIGKILL worker-encode || return 1
  wait_for "worker-encode to die" 30 stopped "$cid" || return 1
  mapfile -t dead < <(temp_files)
  expect "worker-encode was killed (code $(exit_code "$cid")) and left ${#dead[@]} temp files in tmp/" \
    test "${#dead[@]}" -ge 2 || ok=1

  log "waiting out the ${lease}s lease, plus ${lease_margin}s"
  sleep $((lease + lease_margin))
  log "restarting worker-encode"
  start_encode || return 1
  wait_for "asset $asset_b to be ready" 600 state_is "$asset_b" ready || ok=1

  expect "asset is $(state_text "$asset_b"), want ready with attempts 2: reaped and processed again" \
    state_is "$asset_b" ready 2 || ok=1
  for f in "${dead[@]}"; do
    [[ -e $f ]] && left+=("${f##*/}")
  done
  swept=$(last_log_value "swept temp files" removed) || swept=none
  expect "the restart swept $swept temp files, and none of the dead attempt's ${#dead[@]} remain${left[*]:+: ${left[*]}}" \
    test "${#left[@]}" -eq 0 || ok=1
  return "$ok"
}

# Check 3: with a stop timeout below the shutdown grace, check 1 fails: Docker kills worker-encode mid-drain
# and the asset isn't released. This is why stop_grace_period exceeds the grace.
check_short_stop() {
  local cid asset_c attempts start took code ok=0
  wait_for "the earlier assets to finish encoding" 600 none_processing || return 1
  asset_c=$(upload "Process checks: short stop") || return 1
  wait_for "asset $asset_c to be encoding" 60 state_is "$asset_c" processing || return 1
  attempts=$(attempts_of "$asset_c") || return 1
  cid=$(encode_container) || return 1

  log "stopping worker-encode with a ${short_stop}s timeout while asset $asset_c is encoding (attempt $attempts)"
  start=$(now_ms)
  compose stop -t "$short_stop" worker-encode || return 1
  took=$(since "$start")
  code=$(exit_code "$cid") || return 1

  expect "worker-encode was killed after ${took}s with code $code, want 137 (SIGKILL)" test "$code" -eq 137 || ok=1
  expect "asset is $(state_text "$asset_c"), want processing with attempts $attempts: not released" \
    state_is "$asset_c" processing "$attempts" || ok=1
  return "$ok"
}

main() {
  mkdir -p "$work"
  trap teardown EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM
  prepare

  run_check "1. compose stop mid-encode releases the asset and exits 0 within stop_grace_period" check_stop
  run_check "4. a second SIGTERM during the drain ends worker-encode at once" check_second_sigterm
  run_check "2. SIGKILL mid-encode, then a restart after the lease: reaped as attempt 2, temp files swept" check_sigkill
  run_check "3. a stop timeout below the shutdown grace makes check 1 fail" check_short_stop

  echo
  printf '%s\n' "${results[@]}"
  echo "$((${#results[@]} - failures)) of ${#results[@]} checks passed in ${SECONDS}s"
  ((failures == 0))
}

main "$@"
