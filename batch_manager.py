#!/usr/bin/env python3
"""
Xiebo Manager - Simple batch manager for xiebo binary
"""

import subprocess
import json
import os
import sys
import time
import math
from datetime import datetime

# Konfigurasi
BATCH_SIZE = 100000000  # 100 juta keys per batch
LOG_FILE = "xiebo_progress.json"

def load_progress():
    """Load progress from JSON file"""
    if os.path.exists(LOG_FILE):
        try:
            with open(LOG_FILE, 'r') as f:
                return json.load(f)
        except:
            pass
    return None

def save_progress(data):
    """Save progress to JSON file"""
    with open(LOG_FILE, 'w') as f:
        json.dump(data, f, indent=2)

def create_batches(start_hex, range_bits, gpu_id, address):
    """Create batches for the search range"""
    start_hex = start_hex.lower().replace('0x', '').strip()
    
    try:
        start_int = int(start_hex, 16)
    except:
        print(f"ERROR: Invalid hex: {start_hex}")
        return None
    
    total_keys = 1 << range_bits  # 2^range_bits
    end_int = start_int + total_keys - 1
    
    print(f"\n🔍 SEARCH PARAMETERS:")
    print(f"  Start: 0x{format(start_int, 'x')}")
    print(f"  Range: {range_bits} bits")
    print(f"  Total keys: {total_keys:,}")
    print(f"  End: 0x{format(end_int, 'x')}")
    print(f"  Batch size: {BATCH_SIZE:,} keys")
    
    # Calculate batches
    num_batches = math.ceil(total_keys / BATCH_SIZE)
    batches = []
    
    for i in range(num_batches):
        batch_start = start_int + (i * BATCH_SIZE)
        batch_end = min(batch_start + BATCH_SIZE, end_int + 1)
        batch_keys = batch_end - batch_start
        
        # Calculate bits needed
        if batch_keys <= 1:
            batch_bits = 1
        else:
            batch_bits = math.ceil(math.log2(batch_keys))
        
        batches.append({
            'id': i,
            'gpu_id': gpu_id,
            'start_hex': format(batch_start, 'x'),
            'bits': batch_bits,
            'keys': batch_keys,
            'status': 'pending',
            'start_time': None,
            'end_time': None,
            'log_file': f"batch_{i:04d}.log"
        })
        
        # Print first 3 and last batch
        if i < 3 or i == num_batches - 1:
            prefix = "First" if i == 0 else "Last" if i == num_batches - 1 else "Next"
            print(f"  {prefix} batch {i}: 0x{batches[-1]['start_hex']} [{batch_bits} bits, {batch_keys:,} keys]")
    
    print(f"\n📊 Created {num_batches} batches")
    
    progress_data = {
        'gpu_id': gpu_id,
        'start_hex': start_hex,
        'range_bits': range_bits,
        'address': address,
        'batches': batches,
        'created_at': datetime.now().isoformat(),
        'total_batches': num_batches,
        'completed_batches': 0,
        'status': 'running'
    }
    
    save_progress(progress_data)
    return progress_data

def run_batch(batch):
    """Run a single batch"""
    batch_id = batch['id']
    gpu_id = batch['gpu_id']
    start_hex = batch['start_hex']
    bits = batch['bits']
    address = progress_data['address']
    
    print(f"\n{'='*60}")
    print(f"🚀 BATCH {batch_id}")
    print(f"{'='*60}")
    print(f"GPU: {gpu_id}")
    print(f"Start: 0x{start_hex}")
    print(f"Range bits: {bits}")
    print(f"Keys: {batch['keys']:,}")
    print(f"Log: {batch['log_file']}")
    print(f"{'='*60}")
    
    # Build command
    cmd = ["./xiebo", "-gpuId", str(gpu_id), "-start", start_hex, "-range", str(bits), address]
    print(f"Command: {' '.join(cmd)}")
    print()
    
    # Update batch status
    batch['status'] = 'running'
    batch['start_time'] = datetime.now().isoformat()
    save_progress(progress_data)
    
    # Run the command
    try:
        with open(batch['log_file'], 'w') as log_file:
            # Write header
            log_file.write(f"=== Batch {batch_id} ===\n")
            log_file.write(f"Time: {batch['start_time']}\n")
            log_file.write(f"Command: {' '.join(cmd)}\n")
            log_file.write("=" * 60 + "\n\n")
            log_file.flush()
            
            # Run process
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1
            )
            
            # Stream output
            for line in process.stdout:
                log_file.write(line)
                log_file.flush()
                sys.stdout.write(f"[Batch {batch_id}] {line}")
                sys.stdout.flush()
            
            # Wait for completion
            process.wait()
            exit_code = process.returncode
        
        batch['end_time'] = datetime.now().isoformat()
        
        if exit_code == 0:
            batch['status'] = 'completed'
            print(f"\n✅ Batch {batch_id} completed")
        else:
            batch['status'] = 'failed'
            print(f"\n⚠️  Batch {batch_id} failed (code: {exit_code})")
        
        # Check if key was found
        with open(batch['log_file'], 'r') as f:
            content = f.read()
            if 'found' in content.lower() or 'key' in content.lower():
                print(f"🎉 POSSIBLE KEY FOUND in batch {batch_id}! Check {batch['log_file']}")
        
    except Exception as e:
        print(f"\n❌ Error in batch {batch_id}: {e}")
        batch['status'] = 'failed'
        batch['end_time'] = datetime.now().isoformat()
    
    save_progress(progress_data)
    return batch['status']

def show_status():
    """Show current status"""
    data = load_progress()
    if not data:
        print("❌ No active search found")
        return
    
    print(f"\n{'='*60}")
    print(f"📊 XIEBO STATUS")
    print(f"{'='*60}")
    print(f"GPU: {data['gpu_id']}")
    print(f"Start: 0x{data['start_hex']}")
    print(f"Range: {data['range_bits']} bits")
    print(f"Address: {data['address']}")
    print(f"Created: {data['created_at']}")
    
    batches = data['batches']
    completed = sum(1 for b in batches if b['status'] == 'completed')
    running = sum(1 for b in batches if b['status'] == 'running')
    pending = sum(1 for b in batches if b['status'] == 'pending')
    failed = sum(1 for b in batches if b['status'] == 'failed')
    
    total = len(batches)
    percent = (completed / total * 100) if total > 0 else 0
    
    print(f"\n📈 PROGRESS: {percent:.1f}%")
    print(f"✅ Completed: {completed}/{total}")
    print(f"🔄 Running: {running}")
    print(f"⏳ Pending: {pending}")
    print(f"❌ Failed: {failed}")
    
    # Show next pending batch
    for batch in batches:
        if batch['status'] == 'pending':
            print(f"\n⏭️  Next batch: {batch['id']} (0x{batch['start_hex']}, {batch['keys']:,} keys)")
            break
    
    print(f"{'='*60}")

def resume_search():
    """Resume existing search"""
    data = load_progress()
    if not data:
        print("❌ No search to resume")
        return None
    
    print(f"\n🔄 RESUMING SEARCH")
    print(f"GPU: {data['gpu_id']}")
    print(f"Start: 0x{data['start_hex']}")
    print(f"Range: {data['range_bits']} bits")
    print(f"Address: {data['address']}")
    
    # Reset any running batches to pending (in case of crash)
    for batch in data['batches']:
        if batch['status'] == 'running':
            batch['status'] = 'pending'
            print(f"  Reset batch {batch['id']} from running to pending")
    
    data['status'] = 'running'
    save_progress(data)
    
    return data

def main():
    """Main function"""
    # Parse arguments
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python3 xiebo_manager.py --status")
        print("  python3 xiebo_manager.py --resume")
        print("  python3 xiebo_manager.py --new GPU_ID START_HEX RANGE_BITS ADDRESS")
        sys.exit(1)
    
    global progress_data
    
    mode = sys.argv[1]
    
    if mode == '--status':
        show_status()
        sys.exit(0)
    
    elif mode == '--resume':
        progress_data = resume_search()
        if not progress_data:
            sys.exit(1)
    
    elif mode == '--new':
        if len(sys.argv) != 6:
            print("Usage: python3 xiebo_manager.py --new GPU_ID START_HEX RANGE_BITS ADDRESS")
            sys.exit(1)
        
        gpu_id = int(sys.argv[2])
        start_hex = sys.argv[3]
        range_bits = int(sys.argv[4])
        address = sys.argv[5]
        
        print(f"\n🆕 NEW SEARCH")
        progress_data = create_batches(start_hex, range_bits, gpu_id, address)
        if not progress_data:
            sys.exit(1)
    
    else:
        print(f"Unknown mode: {mode}")
        sys.exit(1)
    
    # Run batches
    print(f"\n{'='*60}")
    print(f"STARTING BATCH EXECUTION")
    print(f"{'='*60}")
    
    start_time = time.time()
    
    try:
        for batch in progress_data['batches']:
            if batch['status'] == 'pending':
                status = run_batch(batch)
                
                # Small delay between batches
                if status != 'failed' and any(b['status'] == 'pending' for b in progress_data['batches']):
                    print(f"\n⏱️  Waiting 3 seconds...")
                    time.sleep(3)
        
        # All batches done
        elapsed = time.time() - start_time
        print(f"\n{'='*60}")
        print(f"✅ ALL BATCHES COMPLETED!")
        print(f"Time: {elapsed:.1f} seconds")
        print(f"{'='*60}")
        
        # Check for found keys
        print(f"\n🔍 Scanning logs for found keys...")
        found = []
        for batch in progress_data['batches']:
            if os.path.exists(batch['log_file']):
                with open(batch['log_file'], 'r') as f:
                    if 'found' in f.read().lower():
                        found.append(batch['log_file'])
        
        if found:
            print(f"🎉 Found keys in these logs:")
            for f in found:
                print(f"  - {f}")
        else:
            print("No keys found")
    
    except KeyboardInterrupt:
        print(f"\n\n⚠️  INTERRUPTED BY USER")
        print("Progress saved. Run with --resume to continue.")
    
    except Exception as e:
        print(f"\n❌ ERROR: {e}")

if __name__ == "__main__":
    main()
