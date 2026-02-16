#!/bin/bash
#
# Load Inferno test data into HFS
#
# Usage: ./install.sh [HFS_URL]
#
# Environment variables:
#   HFS_PORT - Port where HFS is running (default: 8080)
#   HFS_HOST - Host where HFS is running (default: localhost)
#   HFS_URL  - Full URL to HFS (overrides HFS_HOST and HFS_PORT)
#
# Examples:
#   ./install.sh                           # Uses localhost:8080
#   HFS_PORT=8088 ./install.sh             # Uses localhost:8088
#   ./install.sh http://localhost:9000     # Uses specified URL
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Determine HFS URL
if [ -n "$1" ]; then
    HFS_URL="$1"
elif [ -n "$HFS_URL" ]; then
    : # Use HFS_URL from environment
else
    HFS_HOST="${HFS_HOST:-localhost}"
    HFS_PORT="${HFS_PORT:-8080}"
    HFS_URL="http://${HFS_HOST}:${HFS_PORT}"
fi

echo "Loading Inferno test data into HFS at ${HFS_URL}..."
echo ""

FAILED=0
SUCCESS=0
SKIPPED=0

for FILE in "$SCRIPT_DIR"/*.json; do
    FILENAME=$(basename "$FILE")
    echo "Processing $FILENAME..."

    # Determine resource type and endpoint
    RESOURCE_TYPE=$(jq -r '.resourceType // empty' "$FILE")
    BUNDLE_TYPE=$(jq -r '.type // empty' "$FILE")

    if [ "$BUNDLE_TYPE" = "transaction" ]; then
        ENDPOINT="/"
        echo "  Type: transaction bundle -> POST $ENDPOINT"
    elif [ "$RESOURCE_TYPE" = "SearchParameter" ]; then
        ENDPOINT="/SearchParameter"
        echo "  Type: SearchParameter -> POST $ENDPOINT"
    elif [ "$RESOURCE_TYPE" = "Group" ]; then
        ENDPOINT="/Group"
        echo "  Type: Group -> POST $ENDPOINT"
    else
        echo "  WARNING: Unknown type (resourceType=$RESOURCE_TYPE, type=$BUNDLE_TYPE), skipping"
        SKIPPED=$((SKIPPED + 1))
        continue
    fi

    RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${HFS_URL}${ENDPOINT}" \
        -H "Content-Type: application/fhir+json" \
        -d @"$FILE")

    HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
    BODY=$(echo "$RESPONSE" | sed '$d')

    if [ "$HTTP_CODE" -ge 200 ] && [ "$HTTP_CODE" -lt 300 ]; then
        echo "  Success (HTTP $HTTP_CODE)"
        SUCCESS=$((SUCCESS + 1))
    else
        echo "  FAILED (HTTP $HTTP_CODE)"
        echo "  $BODY" | head -c 500
        echo ""
        FAILED=1
    fi
done

echo ""
echo "Summary: $SUCCESS succeeded, $SKIPPED skipped"

if [ "$FAILED" -eq 1 ]; then
    echo "One or more files failed to load"
    exit 1
fi

echo "All test data loaded successfully"
