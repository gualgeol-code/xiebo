#!/bin/bash
# run_batch.sh - Simple wrapper untuk batch manager

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_SCRIPT="batch_manager.py"

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "Error: python3 not found"
    exit 1
fi

# Check if xiebo binary exists
if [ ! -f "./xiebo" ]; then
    echo "Error: xiebo binary not found in current directory"
    echo "Please copy xiebo executable here or update XIEBO_BINARY in batch_manager.py"
    exit 1
fi

# Default parameters
GPU_ID=0
START_HEX="100000000000000000"
RANGE_BITS=68
ADDRESS="19vkiEajfhuZ8bs8Zu2jgmC6oqZbWqhxhG"
MODE="run"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --gpu)
            GPU_ID="$2"
            shift 2
            ;;
        --start)
            START_HEX="$2"
            shift 2
            ;;
        --range)
            RANGE_BITS="$2"
            shift 2
            ;;
        --address)
            ADDRESS="$2"
            shift 2
            ;;
        --resume)
            MODE="resume"
            shift
            ;;
        --status)
            MODE="status"
            shift
            ;;
        --retry)
            MODE="retry"
            shift
            ;;
        --monitor)
            MONITOR="--monitor"
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  --gpu ID          GPU ID (default: 0)"
            echo "  --start HEX       Start key in hex (default: $START_HEX)"
            echo "  --range BITS      Range in bits (default: $RANGE_BITS)"
            echo "  --address ADDR    Target address (default: $ADDRESS)"
            echo "  --resume          Resume from last session"
            echo "  --status          Show status only"
            echo "  --retry           Retry failed batches"
            echo "  --monitor         Monitor progress in background"
            echo "  -h, --help        Show this help"
            echo ""
            echo "Examples:"
            echo "  $0 --gpu 0 --start 100000000000000000 --range 68 --address 19vkiE..."
            echo "  $0 --resume --monitor"
            echo "  $0 --status"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use -h for help"
            exit 1
            ;;
    esac
done

# Run appropriate mode
case $MODE in
    run)
        echo "Starting new batch processing..."
        python3 "$SCRIPT_DIR/$PYTHON_SCRIPT" \
            --gpu "$GPU_ID" \
            --start "$START_HEX" \
            --range "$RANGE_BITS" \
            --address "$ADDRESS" \
            $MONITOR
        ;;
    resume)
        echo "Resuming batch processing..."
        python3 "$SCRIPT_DIR/$PYTHON_SCRIPT" \
            --gpu "$GPU_ID" \
            --start "$START_HEX" \
            --range "$RANGE_BITS" \
            --address "$ADDRESS" \
            --resume \
            $MONITOR
        ;;
    retry)
        echo "Retrying failed batches..."
        python3 "$SCRIPT_DIR/$PYTHON_SCRIPT" \
            --gpu "$GPU_ID" \
            --start "$START_HEX" \
            --range "$RANGE_BITS" \
            --address "$ADDRESS" \
            --retry-failed \
            $MONITOR
        ;;
    status)
        python3 "$SCRIPT_DIR/$PYTHON_SCRIPT" --status
        ;;
esac
