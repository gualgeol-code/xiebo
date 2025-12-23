#!/bin/bash
# quick_start.sh - Quick start script for xiebo batch processing

echo "=========================================="
echo "   XIEBO BATCH PROCESSING - QUICK START"
echo "=========================================="

# Make scripts executable
chmod +x run_batch.sh batch_manager.py 2>/dev/null || true

# Check for xiebo binary
if [ ! -f "./xiebo" ]; then
    echo "ERROR: xiebo binary not found in current directory!"
    echo ""
    echo "Please copy your xiebo executable to this directory."
    echo "Expected: ./xiebo"
    echo ""
    read -p "Do you want to continue anyway? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Show available GPUs
echo ""
echo "Available GPUs:"
nvidia-smi --list-gpus 2>/dev/null || echo "  (GPU detection not available)"

echo ""
echo "Usage examples:"
echo "1. Start new scan:"
echo "   ./run_batch.sh --gpu 0 --start 100000000000000000 --range 68 --address 19vkiEajfhuZ8bs8Zu2jgmC6oqZbWqhxhG"
echo ""
echo "2. Resume last scan:"
echo "   ./run_batch.sh --resume"
echo ""
echo "3. Show status:"
echo "   ./run_batch.sh --status"
echo ""
echo "4. With progress monitoring:"
echo "   ./run_batch.sh --resume --monitor"
echo ""

# Ask user what to do
read -p "What would you like to do? 
1) Start new scan
2) Resume last scan
3) Show status
4) Exit
Choice: " choice

case $choice in
    1)
        read -p "GPU ID (default: 0): " gpu
        read -p "Start hex (default: 100000000000000000): " start
        read -p "Range bits (default: 68): " range
        read -p "Target address (default: 19vkiEajfhuZ8bs8Zu2jgmC6oqZbWqhxhG): " address
        
        gpu=${gpu:-0}
        start=${start:-100000000000000000}
        range=${range:-68}
        address=${address:-19vkiEajfhuZ8bs8Zu2jgmC6oqZbWqhxhG}
        
        ./run_batch.sh --gpu "$gpu" --start "$start" --range "$range" --address "$address" --monitor
        ;;
    2)
        ./run_batch.sh --resume --monitor
        ;;
    3)
        ./run_batch.sh --status
        ;;
    4)
        exit 0
        ;;
    *)
        echo "Invalid choice"
        exit 1
        ;;
esac
