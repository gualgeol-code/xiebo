#!/usr/bin/env python3
"""
Batch Manager untuk executable xiebo binary
Membagi range besar menjadi batch-batch kecil dan menjalankan xiebo.exe secara sequential
"""

import subprocess
import json
import os
import sys
import time
import shutil
from datetime import datetime
import argparse
import threading
from queue import Queue
import signal

# ==================== KONFIGURASI ====================
XIEBO_BINARY = "./xiebo"  # Path ke executable xiebo
LOG_FILE = "batch_progress.json"
BATCH_SIZE = 100000000  # 100 juta keys per batch (sesuaikan dengan kemampuan xiebo)
MAX_PARALLEL_GPU = 1     # Jumlah GPU yang bisa dipakai parallel

# ==================== BATCH MANAGER ====================
class XieboBatchManager:
    def __init__(self, xiebo_binary, log_file=LOG_FILE):
        self.xiebo_binary = xiebo_binary
        self.log_file = log_file
        self.batches = []
        self.load_progress()
        
        # Verifikasi xiebo binary
        if not os.path.exists(xiebo_binary):
            print(f"❌ Error: xiebo binary not found at {xiebo_binary}")
            print("Please place xiebo executable in the same directory or update XIEBO_BINARY path")
            sys.exit(1)
        
        # Test xiebo binary
        self.test_xiebo()
    
    def test_xiebo(self):
        """Test if xiebo binary works"""
        try:
            result = subprocess.run([self.xiebo_binary, "--help"], 
                                  capture_output=True, text=True, timeout=2)
            if result.returncode == 0 or "--help" in result.stderr or "--help" in result.stdout:
                print(f"✅ xiebo binary verified: {self.xiebo_binary}")
                return True
        except:
            pass
        
        try:
            result = subprocess.run([self.xiebo_binary], 
                                  capture_output=True, text=True, timeout=2)
            print(f"✅ xiebo binary verified (exit code: {result.returncode})")
            return True
        except Exception as e:
            print(f"⚠️  xiebo binary test: {e}")
            # Continue anyway, might need different parameters
        
        return True
    
    def load_progress(self):
        """Load progress from log file"""
        if os.path.exists(self.log_file):
            try:
                with open(self.log_file, 'r') as f:
                    data = json.load(f)
                    self.batches = data.get('batches', [])
                    print(f"📂 Loaded {len(self.batches)} batches from {self.log_file}")
            except:
                self.batches = []
        else:
            self.batches = []
    
    def save_progress(self):
        """Save progress to log file"""
        with open(self.log_file, 'w') as f:
            json.dump({
                'batches': self.batches,
                'last_updated': datetime.now().isoformat()
            }, f, indent=2)
    
    def create_batches(self, start_hex, range_bits, gpu_id, target_address):
        """
        Create batch configuration
        start_hex: starting key in hex (e.g., "100000000000000000")
        range_bits: number of bits to search (e.g., 68)
        """
        # Calculate total keys
        start_int = int(start_hex, 16)
        total_keys = 1 << range_bits  # 2^range_bits
        end_int = start_int + total_keys - 1
        
        print(f"\n📊 Batch Planning:")
        print(f"  Start: 0x{start_hex} ({start_int})")
        print(f"  Range: {range_bits} bits")
        print(f"  Total keys: {total_keys:,}")
        print(f"  End: 0x{format(end_int, 'x')} ({end_int})")
        print(f"  Batch size: {BATCH_SIZE:,} keys")
        
        # Create batches
        num_batches = (total_keys + BATCH_SIZE - 1) // BATCH_SIZE
        
        self.batches = []
        for i in range(num_batches):
            batch_start = start_int + (i * BATCH_SIZE)
            batch_end = min(batch_start + BATCH_SIZE, end_int + 1)
            batch_size = batch_end - batch_start
            
            # Format untuk xiebo: start dalam hex, range dalam keys
            # Karena xiebo menerima -range dalam bits, kita perlu convert
            # Tapi jika xiebo menerima -range sebagai jumlah keys, kita hitung bits needed
            
            # Hitung bits yang diperlukan untuk batch_size keys
            # batch_bits = ceil(log2(batch_size))
            batch_bits = (batch_size - 1).bit_length()
            
            batch_info = {
                'id': i,
                'gpu_id': gpu_id,
                'start_hex': format(batch_start, 'x'),  # Tanpa leading zeros
                'batch_bits': batch_bits,
                'batch_size': batch_size,
                'actual_start': batch_start,
                'actual_end': batch_end - 1,
                'target_address': target_address,
                'status': 'pending',  # pending, running, completed, failed
                'start_time': None,
                'end_time': None,
                'output_file': f"batch_{i:04d}.log"
            }
            self.batches.append(batch_info)
            
            if i < 3 or i == num_batches - 1:
                status = "First" if i == 0 else "Last" if i == num_batches - 1 else "Next"
                print(f"  {status} batch {i}: 0x{format(batch_start, 'x')} "
                      f"[{batch_bits} bits, {batch_size:,} keys]")
        
        self.save_progress()
        return self.batches
    
    def execute_batch(self, batch_info):
        """Execute a single batch using xiebo binary"""
        batch_id = batch_info['id']
        output_file = batch_info['output_file']
        
        print(f"\n{'='*60}")
        print(f"🚀 Starting batch {batch_id}")
        print(f"  GPU: {batch_info['gpu_id']}")
        print(f"  Start: 0x{batch_info['start_hex']}")
        print(f"  Range bits: {batch_info['batch_bits']}")
        print(f"  Size: {batch_info['batch_size']:,} keys")
        print(f"  Output: {output_file}")
        print(f"{'='*60}")
        
        # Build command for xiebo binary
        cmd = [
            self.xiebo_binary,
            "-gpuId", str(batch_info['gpu_id']),
            "-start", batch_info['start_hex'],
            "-range", str(batch_info['batch_bits']),
            batch_info['target_address']
        ]
        
        print(f"Command: {' '.join(cmd)}")
        
        # Update batch status
        batch_info['status'] = 'running'
        batch_info['start_time'] = datetime.now().isoformat()
        self.save_progress()
        
        try:
            # Run xiebo with output redirection
            with open(output_file, 'w') as f:
                f.write(f"Batch {batch_id} started at {batch_info['start_time']}\n")
                f.write(f"Command: {' '.join(cmd)}\n")
                f.write("=" * 60 + "\n")
                f.flush()
                
                # Execute and capture output in real-time
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                    universal_newlines=True
                )
                
                # Stream output to file and console
                for line in process.stdout:
                    f.write(line)
                    f.flush()
                    # Print to console with batch ID prefix
                    print(f"[Batch {batch_id}] {line}", end='')
                
                process.wait()
                return_code = process.returncode
            
            batch_info['end_time'] = datetime.now().isoformat()
            
            if return_code == 0:
                batch_info['status'] = 'completed'
                print(f"✅ Batch {batch_id} completed successfully")
            else:
                batch_info['status'] = 'failed'
                print(f"❌ Batch {batch_id} failed with exit code {return_code}")
                
                # Check if key was found (special exit code maybe?)
                with open(output_file, 'r') as f:
                    content = f.read()
                    if "FOUND" in content.upper() or "KEY" in content.upper():
                        print(f"🎉 Possible key found in batch {batch_id}! Check {output_file}")
            
            self.save_progress()
            return return_code
            
        except Exception as e:
            print(f"❌ Error executing batch {batch_id}: {e}")
            batch_info['status'] = 'failed'
            batch_info['end_time'] = datetime.now().isoformat()
            self.save_progress()
            return -1
    
    def get_next_pending_batch(self):
        """Get the next pending batch to execute"""
        for batch in self.batches:
            if batch['status'] == 'pending':
                return batch
        return None
    
    def get_progress_summary(self):
        """Get overall progress summary"""
        total = len(self.batches)
        if total == 0:
            return {
                'total': 0,
                'completed': 0,
                'running': 0,
                'pending': 0,
                'failed': 0,
                'percentage': 0
            }
        
        completed = sum(1 for b in self.batches if b['status'] == 'completed')
        running = sum(1 for b in self.batches if b['status'] == 'running')
        pending = sum(1 for b in self.batches if b['status'] == 'pending')
        failed = sum(1 for b in self.batches if b['status'] == 'failed')
        
        return {
            'total': total,
            'completed': completed,
            'running': running,
            'pending': pending,
            'failed': failed,
            'percentage': (completed / total * 100) if total > 0 else 0
        }
    
    def print_status(self):
        """Print current status"""
        summary = self.get_progress_summary()
        
        print(f"\n{'='*60}")
        print(f"XIEBO BATCH MANAGER STATUS")
        print(f"{'='*60}")
        print(f"Total batches: {summary['total']}")
        print(f"Completed: {summary['completed']} ({summary['percentage']:.1f}%)")
        print(f"Running: {summary['running']}")
        print(f"Pending: {summary['pending']}")
        print(f"Failed: {summary['failed']}")
        
        if summary['total'] > 0:
            # Show recent batches
            print(f"\nRecent batches:")
            for batch in self.batches[-5:]:
                status_icon = {
                    'completed': '✅',
                    'running': '🔄',
                    'pending': '⏳',
                    'failed': '❌'
                }.get(batch['status'], '❓')
                
                print(f"  {status_icon} Batch {batch['id']}: {batch['status']} "
                      f"(0x{batch['start_hex'][:16]}... [{batch['batch_bits']} bits])")
    
    def resume_from_failed(self):
        """Resume from failed batches"""
        failed_batches = [b for b in self.batches if b['status'] == 'failed']
        if not failed_batches:
            print("No failed batches to resume.")
            return
        
        print(f"\n🔄 Resuming {len(failed_batches)} failed batches:")
        for batch in failed_batches:
            batch['status'] = 'pending'
            print(f"  Batch {batch['id']} reset to pending")
        
        self.save_progress()
    
    def monitor_progress(self, interval=10):
        """Monitor progress in a separate thread"""
        print(f"\n📈 Starting progress monitor (updates every {interval}s)")
        print("Press Ctrl+C to stop\n")
        
        try:
            while True:
                self.print_status()
                time.sleep(interval)
                
                # Check if all batches are done
                summary = self.get_progress_summary()
                if summary['pending'] == 0 and summary['running'] == 0:
                    print("\n🎉 All batches completed!")
                    break
                    
        except KeyboardInterrupt:
            print("\n⏸️  Progress monitoring stopped by user")
            return

# ==================== MAIN EXECUTION ====================
def main():
    parser = argparse.ArgumentParser(description='Batch Manager for xiebo binary')
    parser.add_argument('--gpu', type=int, required=True, help='GPU ID to use')
    parser.add_argument('--start', type=str, required=True, help='Start key in hex')
    parser.add_argument('--range', type=int, dest='range_bits', required=True, 
                       help='Range in bits (e.g., 68 for 2^68 keys)')
    parser.add_argument('--address', type=str, required=True, help='Target Bitcoin address')
    parser.add_argument('--resume', action='store_true', help='Resume from previous session')
    parser.add_argument('--status', action='store_true', help='Show status only')
    parser.add_argument('--retry-failed', action='store_true', help='Retry failed batches')
    parser.add_argument('--monitor', action='store_true', help='Monitor progress in background')
    
    args = parser.parse_args()
    
    # Initialize manager
    manager = XieboBatchManager(XIEBO_BINARY)
    
    # Show status only
    if args.status:
        manager.print_status()
        return
    
    # Retry failed batches
    if args.retry_failed:
        manager.resume_from_failed()
        args.resume = True
    
    # Resume from existing batches
    if args.resume:
        if not manager.batches:
            print("No existing batches to resume. Creating new batches...")
            manager.create_batches(args.start, args.range_bits, args.gpu, args.address)
    else:
        # Create new batches
        manager.create_batches(args.start, args.range_bits, args.gpu, args.address)
    
    # Start monitoring in background thread if requested
    monitor_thread = None
    if args.monitor:
        monitor_thread = threading.Thread(target=manager.monitor_progress, daemon=True)
        monitor_thread.start()
    
    # Process batches
    print(f"\n{'='*60}")
    print(f"STARTING BATCH PROCESSING")
    print(f"{'='*60}")
    
    total_start_time = time.time()
    
    try:
        while True:
            batch = manager.get_next_pending_batch()
            if not batch:
                print("\n✅ No more pending batches.")
                break
            
            # Execute batch
            result = manager.execute_batch(batch)
            
            # Small delay between batches
            if manager.get_next_pending_batch():
                print(f"\n⏱️  Waiting 2 seconds before next batch...")
                time.sleep(2)
    
    except KeyboardInterrupt:
        print("\n⚠️  Batch processing interrupted by user.")
        print("Progress has been saved. Use --resume to continue.")
    
    finally:
        total_elapsed = time.time() - total_start_time
        
        # Final status
        summary = manager.get_progress_summary()
        print(f"\n{'='*60}")
        print(f"PROCESSING COMPLETE")
        print(f"{'='*60}")
        print(f"Total time: {total_elapsed:.2f} seconds")
        print(f"Total batches: {summary['total']}")
        print(f"Completed: {summary['completed']}")
        print(f"Failed: {summary['failed']}")
        
        if summary['failed'] > 0:
            print(f"\n⚠️  Some batches failed. Use --retry-failed to retry them.")
        
        # Check for any found keys
        print(f"\n🔍 Checking for found keys...")
        found_files = []
        for filename in os.listdir('.'):
            if filename.startswith('batch_') and filename.endswith('.log'):
                with open(filename, 'r') as f:
                    content = f.read()
                    if any(keyword in content.upper() for keyword in ['FOUND', 'KEY', 'SUCCESS']):
                        found_files.append(filename)
        
        if found_files:
            print(f"🎉 Check these files for possible found keys:")
            for f in found_files:
                print(f"  - {f}")
        else:
            print("No keys found in this session.")

if __name__ == "__main__":
    main()
