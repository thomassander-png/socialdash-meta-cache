#!/bin/bash
# Local development script for SocialDash Meta Cache
# Usage: ./scripts/run_local.sh [mode] [options]

set -e

# Check if .env file exists
if [ -f .env ]; then
    echo "Loading environment from .env file..."
    export $(cat .env | grep -v '^#' | xargs)
fi

# Validate required environment variables
if [ -z "$META_ACCESS_TOKEN" ]; then
    echo "Error: META_ACCESS_TOKEN is not set"
    echo "Set it via environment variable or .env file"
    exit 1
fi

if [ -z "$DATABASE_URL" ]; then
    echo "Error: DATABASE_URL is not set"
    exit 1
fi

if [ -z "$FB_PAGE_IDS" ]; then
    echo "Error: FB_PAGE_IDS is not set"
    exit 1
fi

# Default values
export META_API_VERSION="${META_API_VERSION:-v20.0}"
export TZ="${TZ:-Europe/Berlin}"

# Run the command
MODE="${1:-cache}"

case "$MODE" in
    cache)
        echo "Running cache operation..."
        python -m src.main --mode cache
        ;;
    backfill)
        if [ -z "$2" ] || [ -z "$3" ]; then
            echo "Usage: ./run_local.sh backfill START_DATE END_DATE"
            echo "Example: ./run_local.sh backfill 2025-12-01 2025-12-31"
            exit 1
        fi
        echo "Running backfill from $2 to $3..."
        python -m src.main --mode backfill --start "$2" --end "$3"
        ;;
    finalize)
        if [ -z "$2" ]; then
            echo "Usage: ./run_local.sh finalize MONTH"
            echo "Example: ./run_local.sh finalize 2025-12"
            exit 1
        fi
        echo "Finalizing month $2..."
        python -m src.main --mode finalize_month --month "$2"
        ;;
    migrate)
        echo "Running migrations..."
        python -m src.main --mode migrate
        ;;
    test)
        echo "Running tests..."
        pytest tests/ -v
        ;;
    *)
        echo "Unknown mode: $MODE"
        echo "Available modes: cache, backfill, finalize, migrate, test"
        exit 1
        ;;
esac

echo "Done!"
