#!/usr/bin/env bash
# Run the TORA L2 capture for N seconds; raw JSONL → captures/{date}/raw/.
# usage: capture_live.sh [seconds] [front] [acct_type] [sids_csv] [resend]
#   seconds  default 7200 (a full session)
#   front    default tcp://210.14.72.16:9402  (7x24; sim = tcp://210.14.72.21:4402)
#   acct_type '0' UserID | '1' 资金账号 (DepartmentID via TORA_DEPARTMENT env)
# Requires: lev2_dump built (see cpp/lev2_dump.cpp), alignment_ob/secrets.yaml.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SECS="${1:-7200}"
FRONT="${2:-tcp://210.14.72.16:9402}"
ACCT="${3:-0}"
SIDS="${4:-600000,000001}"
RESEND="${5:-1}"

USER_PW=$("$REPO/.venv-live/bin/python" -c "
import yaml
s = yaml.safe_load(open('$REPO/alignment_ob/secrets.yaml'))
print(s['account']['user'] + ' ' + s['account']['password'])
")
read -r USER PW <<< "$USER_PW"

DATE=$(TZ=Asia/Shanghai date +%Y%m%d)
OUT="$REPO/alignment_ob/captures/$DATE/raw"
mkdir -p "$OUT"
TAG=$(echo "$FRONT" | tr -d ':/')

echo "capture: front=$FRONT acct_type=$ACCT sids=$SIDS seconds=$SECS date=$DATE" >&2
timeout "$SECS" "$REPO/alignment_ob/sdk/bin/lev2_dump" \
  "$FRONT" "$USER" "$PW" "$ACCT" "$SIDS" "$RESEND" 2>/dev/null \
  | grep --line-buffered '^{' > "$OUT/capture_${TAG}.jsonl" || true
echo "captured $(wc -l < "$OUT/capture_${TAG}.jsonl") lines → $OUT/capture_${TAG}.jsonl" >&2
