#!/usr/bin/env python3
"""
Xiebo Runner - FIXED VERSION
Masalah: Program berhenti setelah planning karena menunggu output dari subprocess
Solusi: Gunakan subprocess.run() tanpa capture output
"""

import subprocess
import sys
import os
import time
import math

def run_xiebo(gpu_id, start_hex, range_bits, address):
    """Run xiebo binary directly"""
    cmd = ["./xiebo", "-gpuId", str(gpu_id), "-start", start_hex, "-range", str(range_bits), address]
    
    print(f"\n{'='*60}")
    print(f"Running: {' '.join(cmd)}")
    print(f"{'='*60}")
    
    try:
        # RUN WITHOUT CAPTURING OUTPUT - Ini kunci perbaikan!
        result = subprocess.run(cmd)
        return result.returncode
    except KeyboardInterrupt:
        print("\n⚠️ Stopped by user")
        return 130
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1

def main():
    # Parse arguments directly
    if len(sys.argv) < 2:
        print("Xiebo Batch Runner")
        print("Usage:")
        print("  Single run: python3 xiebo_runner_fixed.py GPU_ID START_HEX RANGE_BITS ADDRESS")
        print("  Batch run:  python3 xiebo_runner_fixed.py --batch GPU_ID START_HEX RANGE_BITS ADDRESS")
        sys.exit(1)
    
    # Single run mode
    if len(sys.argv) == 5:
        gpu_id = sys.argv[1]
        start_hex = sys.argv[2]
        range_bits = int(sys.argv[3])
        address = sys.argv[4]
        
        print(f"Single run mode")
        return run_xiebo(gpu_id, start_hex, range_bits, address)
    
    # Batch run mode
    elif sys.argv[1] == "--batch" and len(sys.argv) == 6:
        gpu_id = sys.argv[2]
        start_hex = sys.argv[3]
        range_bits = int(sys.argv[4])
        address = sys.argv[5]
        
        BATCH_SIZE = 100000000  # 100 juta keys per batch
        
        # Calculate total
        start_int = int(start_hex, 16)
        total_keys = 1 << range_bits
        end_int = start_int + total_keys - 1
        
        print(f"\n{'='*60}")
        print(f"BATCH MODE")
        print(f"{'='*60}")
        print(f"GPU: {gpu_id}")
        print(f"Start: 0x{start_hex}")
        print(f"Range: {range_bits} bits")
        print(f"Total keys: {total_keys:,}")
        print(f"End: 0x{format(end_int, 'x')}")
        print(f"Batch size: {BATCH_SIZE:,} keys")
        
        # Calculate batches
        num_batches = math.ceil(total_keys / BATCH_SIZE)
        
        print(f"\nNumber of batches: {num_batches}")
        print("First 3 batches:")
        
        # Run each batch
        for i in range(num_batches):
            batch_start = start_int + (i * BATCH_SIZE)
            batch_end = min(batch_start + BATCH_SIZE, end_int + 1)
            batch_keys = batch_end - batch_start
            
            # Calculate bits
            if batch_keys <= 1:
                batch_bits = 1
            else:
                batch_bits = math.ceil(math.log2(batch_keys))
            
            batch_hex = format(batch_start, 'x')
            
            if i < 3:
                print(f"  Batch {i}: 0x{batch_hex} [{batch_bits} bits, {batch_keys:,} keys]")
            
            # Run this batch
            print(f"\n▶️  Starting batch {i+1}/{num_batches}")
            print(f"   Start: 0x{batch_hex}")
            print(f"   Bits: {batch_bits}")
            print(f"   Keys: {batch_keys:,}")
            
            return_code = run_xiebo(gpu_id, batch_hex, batch_bits, address)
            
            if return_code == 0:
                print(f"✅ Batch {i+1} completed")
            else:
                print(f"⚠️  Batch {i+1} exited with code {return_code}")
            
            # Delay between batches (except last one)
            if i < num_batches - 1:
                print(f"\n⏱️  Waiting 5 seconds...")
                time.sleep(5)
        
        print(f"\n{'='*60}")
        print(f"✅ ALL BATCHES COMPLETED!")
        print(f"{'='*60}")
        
    else:
        print("Invalid arguments")
        print("Usage: python3 xiebo_runner_fixed.py GPU_ID START_HEX RANGE_BITS ADDRESS")
        print("Or:    python3 xiebo_runner_fixed.py --batch GPU_ID START_HEX RANGE_BITS ADDRESS")
        return 1

if __name__ == "__main__":
    # Check if xiebo exists
    if not os.path.exists("./xiebo"):
        print("❌ Error: xiebo binary not found in current directory")
        print("Please copy xiebo executable to this directory")
        sys.exit(1)
    
    # Check if executable
    if not os.access("./xiebo", os.X_OK):
        print("⚠️  xiebo is not executable, trying to fix...")
        os.chmod("./xiebo", 0o755)
    
    main()
