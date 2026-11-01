#!/usr/bin/env bash
# Runs the public tests T1-T5 plus the bandwidth tests, 3 runs each (one run at
# a time, a crashed run is retried), and writes one summary file per test:
#   results/t1_<label>.txt ... results/t5_<label>.txt results/bandwidth_<label>.txt
# Raw logs go to results/raw/<label>/.
#
#   scripts/run_all.sh before   # naive starter server (git commit $NAIVE_COMMIT)
#   scripts/run_all.sh after    # the solution_server.py in this working tree
#
# Needs: Ubuntu, the venv active (python), sudo + tc. Takes about 30 minutes.
# Do not run two instances at once (shared port 8765, shared netem on lo).
set -u
LABEL=${1:?usage: run_all.sh before|after}
NAIVE_COMMIT=${NAIVE_COMMIT:-dd282f0}
REPO=$(cd "$(dirname "$0")/.." && pwd)
OUT=$REPO/results
RAW=$OUT/raw/$LABEL
mkdir -p "$RAW"

reset() { bash "$REPO/network_profiles/reset.sh" >/dev/null 2>&1; }

# Ask for the sudo password once and keep it alive, so a mid-run prompt cannot hang.
sudo -v || exit 1
( while true; do sudo -n true 2>/dev/null; sleep 50; done ) &
KEEPALIVE=$!
cleanup() { reset; kill "$KEEPALIVE" 2>/dev/null; }
trap cleanup EXIT

if [ "$LABEL" = before ]; then
  WORK=$(mktemp -d)
  ( cd "$REPO" && tar --exclude=./venv --exclude=./.git --exclude=./results -cf - . ) | tar -xf - -C "$WORK"
  ( cd "$REPO" && git show "$NAIVE_COMMIT":solution_server.py ) > "$WORK/solution_server.py" || exit 1
  if grep -q congestion "$WORK/solution_server.py"; then
    echo "commit $NAIVE_COMMIT is not the naive starter server"; exit 1
  fi
  echo "BEFORE = naive starter server from $NAIVE_COMMIT (copy in $WORK)"
else
  WORK=$REPO
  echo "AFTER = $REPO/solution_server.py"
  grep -nE "^MAX_WINDOW|^CONGEST_MIN_AGE_S|^class PathRtt" "$WORK/solution_server.py"
fi
reset
tc qdisc show dev lo | grep -q noqueue || { echo "lo is not clean (expected noqueue)"; exit 1; }

# run_test <target-file> <section> <profile|none|bw500> <run_demo args...>
run_test() {
  local target=$1 section=$2 prof=$3; shift 3
  echo "[$(date +%H:%M:%S)] $LABEL: $section"
  case $prof in
    none) ;;
    bw500) sudo tc qdisc del dev lo root 2>/dev/null
           sudo tc qdisc add dev lo root tbf rate 500kbit burst 16kbit latency 400ms ;;
    *)     bash "$REPO/network_profiles/$prof" >/dev/null ;;
  esac
  if [ "$prof" != none ] && ! tc qdisc show dev lo | grep -qE "netem|tbf"; then
    echo "  NO IMPAIRMENT ACTIVE, skipping $section"; return
  fi
  local i a log
  for i in 1 2 3; do
    for a in 1 2 3 4 5; do
      [ "$(df --output=avail -m / | tail -1)" -lt 100 ] && { echo "DISK LOW, stopping"; exit 1; }
      log=$RAW/${section}_r${i}_a${a}.log
      ( cd "$WORK" && python run_demo.py "$@" --runs 1 --output-stem "${section}_${LABEL}_r${i}" ) > "$log" 2>&1
      if [ "$LABEL" = before ] && grep -q "coalesced=" "$log"; then
        echo "  the naive server was NOT used (found coalesced= in $log)"; exit 1
      fi
      grep -q "load_clients.py failed" "$log" || break
      echo "  crash: $section run $i attempt $a, retrying"
    done
  done
  reset
  python3 "$REPO/scripts/summarize.py" "$section" "$RAW/${section}_r"*_a*.log >> "$OUT/${target}_${LABEL}.txt"
}

for t in t1 t2 t3 t4 t5 bandwidth; do rm -f "$OUT/${t}_${LABEL}.txt"; done
{ echo "# $LABEL, $(date -u +%F\ %H:%M) UTC, $(nproc) vCPU, $(free -m | awk '/Mem:/{print $2}') MB RAM, $(uname -srm)"; } > "$OUT/env_${LABEL}.txt"

run_test t1 t1 none --clients 20  --duration 20
run_test t2 t2 none --clients 100 --duration 40
run_test t3 t3 none --clients 100 --duration 60 --slow-percent 10 --slow-ms 500
run_test t4 t4 none --clients 100 --duration 60 --disconnect-percent 20 --disconnect-at 15 --disconnect-seconds 5
run_test t5 t5_delay100    delay_100ms.sh --clients 100 --duration 40
run_test t5 t5_loss3       loss_3pct.sh   --clients 100 --duration 40
run_test t5 t5_delay_loss  delay_loss.sh  --clients 100 --duration 40
run_test bandwidth bw_2mbit   bandwidth_2mbit.sh --clients 100 --duration 40
run_test bandwidth bw_500kbit bw500             --clients 100 --duration 40

echo "[$(date +%H:%M:%S)] DONE ($LABEL). Summaries: $OUT/*_${LABEL}.txt"
tc qdisc show dev lo
