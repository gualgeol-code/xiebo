#!/usr/bin/env python3
"""
xiebo_simple.py - Super simple batch runner
"""

import subprocess
import sys
import time

def main():
    if len(sys.argv) != 5:
        print("Usage: python3 xiebo_simple.py GPU_ID START_HEX RANGE_BITS ADDRESS")
        sys.exit(1)
    
    gpu_id = sys.argv[1]
    start_hex = sys.argv[2]
    range_bits = int(sys.argv[3])
    address = sys.argv[4]
    
    print(f"Starting xiebo with:")
    print(f"  GPU: {gpu_id}")
    print(f"  Start: {start_hex}")
    print(f"  Range: {range_bits} bits")
    print(f"  Address: {address}")
    print()
    
    # Build command
    cmd = ["./xiebo", "-gpuId", gpu_id, "-start", start_hex, "-range", str(range_bits), address]
    
    print(f"Running: {' '.join(cmd)}")
    print("=" * 60)
    
    try:
        # Just run it directly
        subprocess.run(cmd)
    except KeyboardInterrupt:
        print("\nStopped by user")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
