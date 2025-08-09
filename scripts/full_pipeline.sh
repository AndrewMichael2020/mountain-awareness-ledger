#!/usr/bin/env bash
set -euo pipefail

API_URL=${API_URL:-http://127.0.0.1:8000}
JURS=${JURS:-"WA BC AB"}
YEARS=${YEARS:-5}
MODE=${MODE:-broad}
STRICT=${STRICT:-false}
MAX_PER_QUERY=${MAX_PER_QUERY:-10}
AUGMENT_LIMIT=${AUGMENT_LIMIT:-50}
EXCLUDE_PATTERN=${EXCLUDE_PATTERN:-'youtube.com|facebook.com|twitter.com|x.com|instagram.com|heraldnet.com|kuow.org|spokesman.com|mynorthwest.com|kiro7.com|reddit.com|yahoo.com'}

echo "API=$API_URL JURS=$JURS YEARS=$YEARS MODE=$MODE STRICT=$STRICT MAX=$MAX_PER_QUERY"

for J in $JURS; do
  echo "=== Jurisdiction: $J ==="
  echo "> Discover $J"
  curl -s -X POST "$API_URL/discover?jurisdiction=$J&years=$YEARS&mode=$MODE&strict=$STRICT&max_results_per_query=$MAX_PER_QUERY" \
    | jq -r '.items[].url' \
    | grep -Ev "$EXCLUDE_PATTERN" \
    | sort -u \
    | jq -Rs 'split("\n") | map(select(length>0)) | {urls: .}' > "payload.$J.json"

  COUNT=$(jq '.urls | length' "payload.$J.json")
  echo "Found $COUNT URLs for $J"

  if [ "$COUNT" -gt 0 ]; then
    echo "> Ingest $J ($COUNT)"
    OUT_FILE="ingest.$J.out.json"
    if ! curl -sS --fail-with-body -X POST "$API_URL/ingest/batch" -H "Content-Type: application/json" --data-binary @"payload.$J.json" -o "$OUT_FILE"; then
      echo "Ingest HTTP error for $J; raw body:" >&2
      cat "$OUT_FILE" 2>/dev/null || true
    else
      # Try to parse JSON response; if it fails, show raw body for debugging
      if ! jq '{ok, errors_count:(.errors|length)}' "$OUT_FILE"; then
        echo "Non-JSON ingest response for $J; raw body:" >&2
        cat "$OUT_FILE" >&2
      fi
    fi
  fi

  echo "> Augment missing $J"
  curl -s -X POST "$API_URL/events/augment_missing?jurisdiction=$J&limit=$AUGMENT_LIMIT&force=false" | jq '{attempted, ok, skipped, errors}'

done

echo "Done."