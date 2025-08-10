#!/usr/bin/env bash
set -euo pipefail
# Query Tavily (advanced, single call) and ingest URLs via /ingest/batch using header x-api-key auth only.
# Usage: scripts/tavily_ingest.sh <BC|AB|WA> [years=3] [max_urls=10]

JURIS=${1:-BC}
YEARS=${2:-3}
MAX=${3:-10}

cd "$(dirname "$0")/.."

if [ ! -f .env.local ]; then
  echo "Missing .env.local" >&2
  exit 1
fi

set -a
# shellcheck disable=SC1091
source .env.local
set +a

if [ -z "${TAVILY_API_KEY:-}" ]; then
  echo "TAVILY_API_KEY not set" >&2
  exit 1
fi

case "$JURIS" in
  BC) REGION="British Columbia"; COUNTRY="canada";;
  AB) REGION="Alberta"; COUNTRY="canada";;
  WA) REGION="Washington State"; COUNTRY="united states";;
  *) echo "Unknown jurisdiction: $JURIS" >&2; exit 1;;
esac

INCLUDE_DOMAINS_COMMON=(cbc.ca globalnews.ca avalanche.ca gripped.com gofundme.com)
INCLUDE_DOMAINS_BC=(vancouversun.com theprovince.com squamishchief.com northshorerescue.com squamishsar.org)
INCLUDE_DOMAINS_AB=(calgaryherald.com edmontonjournal.com calgary.ctvnews.ca edmonton.ctvnews.ca)
INCLUDE_DOMAINS_WA=(seattletimes.com king5.com komonews.com heraldnet.com bellinghamherald.com fox13seattle.com mountbakersar.org seattlemountainrescue.org)

DOMAINS=("${INCLUDE_DOMAINS_COMMON[@]}")
case "$JURIS" in
  BC) DOMAINS+=("${INCLUDE_DOMAINS_BC[@]}");;
  AB) DOMAINS+=("${INCLUDE_DOMAINS_AB[@]}");;
  WA) DOMAINS+=("${INCLUDE_DOMAINS_WA[@]}");;
esac

DOM_JSON="[\n$(printf '  \"%s\",\n' "${DOMAINS[@]}" | sed '$ s/,$//')\n]"

if [ "$YEARS" -ge 1 ]; then TIME_RANGE="year"; else TIME_RANGE="month"; fi

QUERY="(climber OR mountaineer OR hiker OR skier) AND (died OR fatal OR killed OR avalanche) AND (${REGION} OR ${JURIS})"

REQ=$(mktemp)
RESP=$(mktemp)
PAY=$(mktemp)
trap 'rm -f "$REQ" "$RESP" "$PAY"' EXIT

cat > "$REQ" <<JSON
{
  "query": "$QUERY",
  "search_depth": "advanced",
  "max_results": 8,
  "time_range": "$TIME_RANGE",
  "country": "$COUNTRY",
  "include_domains": $DOM_JSON
}
JSON

HTTP=$(curl -sS -o "$RESP" -w '%{http_code}' -X POST "https://api.tavily.com/search" \
  -H "Content-Type: application/json" -H "Accept: application/json" -H "User-Agent: alpine-ledger/0.1" \
  -H "x-api-key: ${TAVILY_API_KEY}" \
  --data-binary @"$REQ" || true)
echo "Tavily HTTP(x-api-key)=$HTTP" >&2
if [ "$HTTP" != "200" ]; then
  echo "Tavily error body (first 400 chars):" >&2
  head -c 400 "$RESP" >&2 || true
  exit 1
fi

LEN=$(jq '.results | length' "$RESP" 2>/dev/null || echo 0)
echo "Tavily results: $LEN" >&2
if [ "$LEN" -eq 0 ]; then
  exit 0
fi

URLS=$(jq -r '.results[] | .url | select(.!=null)' "$RESP" | awk 'BEGIN{RS="\n"} {if(!seen[$0]++){print}}' | head -n "$MAX")
if [ -z "$URLS" ]; then
  exit 0
fi

printf '{"urls": [' > "$PAY"
first=1
while IFS= read -r u; do
  if [ $first -eq 1 ]; then first=0; else printf ', ' >> "$PAY"; fi
  printf '"%s"' "$u" >> "$PAY"
done <<< "$URLS"
printf ']}' >> "$PAY"

echo "Posting to /ingest/batch..." >&2
curl -sS -X POST http://127.0.0.1:8000/ingest/batch -H "Content-Type: application/json" --data-binary @"$PAY" | jq '.'

curl -sS "http://127.0.0.1:8000/events?jurisdiction=$JURIS" | jq '{count: (.items | length), items: .items}'
