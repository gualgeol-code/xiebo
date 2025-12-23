#!/usr/bin/env python3
"""
Batch Manager untuk executable xiebo binary - FIXED RESUME LOGIC
"""

import subprocess
import json
import os
import sys
import time
import signal
from datetime import datetime
import argparse
import threading
import math

# ==================== KONFIGURASI ====================
XIEBO_BINARY = "./xiebo"
LOG_FILE = "batch_progress.json"
BATCH_SIZE = 100000000  # 100 juta keys per batch

# ==================== BATCH MANAGER ====================
class XieboBatchManager:
    def __init__(self, xiebo_binary, log_file=LOG_FILE):
        self.xiebo_binary = xiebo_binary
        self.log_file = log_file
        self.batches = []
        self.running = False
        self.should_stop = False
        self.load_progress()
        
        # Setup signal handler
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def signal_handler(self, signum, frame):
        print(f"\n⚠️  Received signal {signum}, stopping gracefully...")
        self.should_stop = True
        self.running = False
    
    def load_progress(self):
        """Load progress from log file"""
        try:
            if os.path.exists(self.log_file):
                with open(self.log_file, 'r') as f:
                    data = json.load(f)
                    self.batches = data.get('batches', [])
                    print(f"📂 Loaded {len(self.batches)} batches from {self.log_file}")
                    return True
            else:
                self.batches = []
                return False
        except Exception as e:
            print(f"⚠️  Error loading progress: {e}")
            self.batches = []
            return False
    
    def save_progress(self):
        """Save progress to log file"""
        try:
            with open(self.log_file, 'w') as f:
                json.dump({
                    'batches': self.batches,
                    'last_updated': datetime.now().isoformat()
                }, f, indent=2, default=str)
        except Exception as e:
            print(f"⚠️  Error saving progress: {e}")
    
    def create_batches(self, start_hex, range_bits, gpu_id, target_address):
        """
        Create batch configuration
        """
        # Clean hex input
        start_hex = start_hex.lower().replace('0x', '').strip()
        
        try:
            start_int = int(start_hex, 16)
        except ValueError:
            print(f"❌ Invalid hex string: {start_hex}")
            return False
        
        total_keys = 1 << range_bits  # 2^range_bits
        end_int = start_int + total_keys - 1
        
        print(f"\n📊 Batch Planning:")
        print(f"  Start: 0x{format(start_int, 'x')} ({start_int:,})")
        print(f"  Range: {range_bits} bits")
        print(f"  Total keys: {total_keys:,}")
        print(f"  End: 0x{format(end_int, 'x')} ({end_int:,})")
        print(f"  Batch size: {BATCH_SIZE:,} keys")
        
        # Calculate number of batches
        num_batches = math.ceil(total_keys / BATCH_SIZE)
        
        self.batches = []
        for i in range(num_batches):
            batch_start = start_int + (i * BATCH_SIZE)
            batch_end = min(batch_start + BATCH_SIZE, end_int + 1)
            batch_size = batch_end - batch_start
            
            # Calculate bits needed for this batch
            if batch_size <= 1:
                batch_bits = 1
            else:
                batch_bits = math.ceil(math.log2(batch_size))
            
            batch_info = {
                'id': i,
                'gpu_id': gpu_id,
                'start_hex': format(batch_start, 'x'),
                'batch_bits': batch_bits,
                'batch_size': batch_size,
                'actual_start': batch_start,
                'actual_end': batch_end - 1,
                'target_address': target_address,
                'status': 'pending',
                'start_time': None,
                'end_time': None,
                'output_file': f"batch_{i:06d}.log"
            }
            self.batches.append(batch_info)
            
            # Print first 3 and last batch
            if i < 3 or i == num_batches - 1:
                status = "First" if i == 0 else "Last" if i == num_batches - 1 else "Next"
                print(f"  {status} batch {i}: 0x{batch_info['start_hex']} "
                      f"[{batch_bits} bits, {batch_size:,} keys]")
        
        self.save_progress()
        print(f"✅ Created {num_batches} batches")
        return True
    
    def execute_batch(self, batch_info):
        """Execute a single batch using xiebo binary"""
        batch_id = batch_info['id']
        
        print(f"\n{'='*60}")
        print(f"🚀 Starting batch {batch_id}")
        print(f"  GPU: {batch_info['gpu_id']}")
        print(f"  Start: 0x{batch_info['start_hex']}")
        print(f"  Range bits: {batch_info['batch_bits']}")
        print(f"  Size: {batch_info['batch_size']:,} keys")
        print(f"{'='*60}")
        
        # Build command
        cmd = [
            self.xiebo_binary,
            "-gpuId", str(batch_info['gpu_id']),
            "-start", batch_info['start_hex'],
            "-range", str(batch_info['batch_bits']),
            batch_info['target_address']
        ]
        
        print(f"Command: {' '.join(cmd)}")
        
        # Update status
        batch_info['status'] = 'running'
        batch_info['start_time'] = datetime.now().isoformat()
        self.save_progress()
        
        # Execute
        try:
            with open(batch_info['output_file'], 'w') as out_file:
                # Write header
                out_file.write(f"=== Batch {batch_id} ===\n")
                out_file.write(f"Start: {batch_info['start_time']}\n")
                out_file.write(f"Command: {' '.join(cmd)}\n")
                out_file.write(f"{'='*60}\n\n")
                out_file.flush()
                
                # Start process
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                    universal_newlines=True
                )
                
                # Read output line by line
                for line in process.stdout:
                    out_file.write(line)
                    out_file.flush()
                    # Print to console with batch prefix
                    print(f"[Batch {batch_id}] {line}", end='')
                
                # Wait for process to complete
                process.wait()
                return_code = process.returncode
            
            batch_info['end_time'] = datetime.now().isoformat()
            
            if return_code == 0:
                batch_info['status'] = 'completed'
                print(f"✅ Batch {batch_id} completed successfully")
            else:
                batch_info['status'] = 'failed'
                print(f"❌ Batch {batch_id} failed with exit code {return_code}")
                
                # Check if key was found
                with open(batch_info['output_file'], 'r') as f:
                    content = f.read()
                    if any(keyword in content for keyword in ['found', 'Found', 'FOUND', 'key', 'Key', 'KEY']):
                        print(f"🎉 KEY FOUND in batch {batch_id}! Check {batch_info['output_file']}")
            
            self.save_progress()
            return return_code
            
        except Exception as e:
            print(f"❌ Error executing batch {batch_id}: {e}")
            batch_info['status'] = 'failed'
            batch_info['end_time'] = datetime.now().isoformat()
            self.save_progress()
            return -1
    
    def get_next_pending_batch(self):
        """Get next pending batch"""
        for batch in self.batches:
            if batch['status'] == 'pending':
                return batch
        return None
    
    def monitor_progress(self):
        """Monitor progress in background"""
        print(f"\n📈 Progress monitor started (Ctrl+C to stop)...")
        
        while self.running and not self.should_stop:
            try:
                self.print_status()
                time.sleep(30)  # Update every 30 seconds
            except KeyboardInterrupt:
                print("\n⏸️  Monitoring stopped")
                break
            except Exception as e:
                print(f"⚠️  Monitor error: {e}")
                time.sleep(5)
    
    def print_status(self):
        """Print current status"""
        total = len(self.batches)
        if total == 0:
            print("No batches to process")
            return
        
        completed = sum(1 for b in self.batches if b['status'] == 'completed')
        running = sum(1 for b in self.batches if b['status'] == 'running')
        pending = sum(1 for b in self.batches if b['status'] == 'pending')
        failed = sum(1 for b in self.batches if b['status'] == 'failed')
        
        percentage = (completed / total * 100) if total > 0 else 0
        
        print(f"\n{'='*60}")
        print(f"📊 STATUS: {percentage:.1f}% ({completed}/{total} batches)")
        print(f"  ✅ Completed: {completed}")
        print(f"  🔄 Running: {running}")
        print(f"  ⏳ Pending: {pending}")
        print(f"  ❌ Failed: {failed}")
        
        # Show current running batch
        for batch in self.batches:
            if batch['status'] == 'running':
                elapsed = ""
                if batch['start_time']:
                    try:
                        start = datetime.fromisoformat(batch['start_time'])
                        elapsed = str(datetime.now() - start).split('.')[0]
                    except:
                        pass
                print(f"  Current: Batch {batch['id']} (running for {elapsed})")
                break
        
        print(f"{'='*60}")
    
    def run_batches(self):
        """Main function to run all batches"""
        if not self.batches:
            print("❌ No batches to process")
            return False
        
        self.running = True
        self.should_stop = False
        
        print(f"\n{'='*60}")
        print(f"STARTING BATCH PROCESSING")
        print(f"Total batches: {len(self.batches)}")
        print(f"{'='*60}")
        
        total_start_time = time.time()
        batches_processed = 0
        
        try:
            while self.running and not self.should_stop:
                batch = self.get_next_pending_batch()
                if not batch:
                    print("\n✅ No more pending batches")
                    break
                
                # Execute batch
                result = self.execute_batch(batch)
                batches_processed += 1
                
                # Check if we should stop
                if self.should_stop:
                    print("\n⏸️  Stopping as requested...")
                    break
                
                # Small delay between batches if not stopping
                if not self.should_stop and self.get_next_pending_batch():
                    print(f"\n⏱️  Waiting 5 seconds before next batch...")
                    for i in range(5, 0, -1):
                        if self.should_stop:
                            break
                        print(f"  {i}...")
                        time.sleep(1)
        
        except KeyboardInterrupt:
            print("\n⚠️  Interrupted by user")
        except Exception as e:
            print(f"\n❌ Error in batch processing: {e}")
        finally:
            self.running = False
            total_elapsed = time.time() - total_start_time
            
            # Final summary
            print(f"\n{'='*60}")
            print(f"PROCESSING FINISHED")
            print(f"{'='*60}")
            print(f"Total time: {total_elapsed:.2f} seconds")
            print(f"Batches processed: {batches_processed}")
            
            summary = self.get_summary()
            print(f"✅ Completed: {summary['completed']}")
            print(f"❌ Failed: {summary['failed']}")
            print(f"⏳ Pending: {summary['pending']}")
            
            # Check for found keys
            self.check_for_found_keys()
            
            return True
    
    def get_summary(self):
        """Get summary of batches"""
        total = len(self.batches)
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
    
    def check_for_found_keys(self):
        """Check log files for found keys"""
        print(f"\n🔍 Checking log files for found keys...")
        found_files = []
        
        for batch in self.batches:
            if os.path.exists(batch['output_file']):
                try:
                    with open(batch['output_file'], 'r') as f:
                        content = f.read()
                        if any(indicator in content.lower() for indicator in 
                              ['found', 'key found', 'private key', 'success']):
                            found_files.append(batch['output_file'])
                except:
                    pass
        
        if found_files:
            print(f"🎉 Potential keys found in these files:")
            for f in found_files:
                print(f"  - {f}")
            
            # Create summary file
            with open('FOUND_KEYS_SUMMARY.txt', 'w') as f:
                f.write("Potential keys found in these batch logs:\n")
                for file in found_files:
                    f.write(f"- {file}\n")
                f.write(f"\nChecked at: {datetime.now().isoformat()}\n")
        else:
            print("No keys found in this session")
    
    def resume_failed_batches(self):
        """Reset failed batches to pending"""
        failed_batches = [b for b in self.batches if b['status'] == 'failed']
        if not failed_batches:
            print("No failed batches to resume")
            return 0
        
        print(f"\n🔄 Resetting {len(failed_batches)} failed batches to pending...")
        for batch in failed_batches:
            batch['status'] = 'pending'
            print(f"  Batch {batch['id']} reset to pending")
        
        self.save_progress()
        return len(failed_batches)

# ==================== MAIN ====================
def main():
    parser = argparse.ArgumentParser(description='Batch Manager for xiebo binary')
    parser.add_argument('--gpu', type=int, help='GPU ID to use')
    parser.add_argument('--start', type=str, help='Start key in hex')
    parser.add_argument('--range', type=int, dest='range_bits', help='Range in bits')
    parser.add_argument('--address', type=str, help='Target Bitcoin address')
    parser.add_argument('--resume', action='store_true', help='Resume from previous session')
    parser.add_argument('--status', action='store_true', help='Show status only')
    parser.add_argument('--retry-failed', action='store_true', help='Retry failed batches')
    parser.add_argument('--monitor', action='store_true', help='Monitor progress in background')
    
    args = parser.parse_args()
    
    # Initialize
    manager = XieboBatchManager(XIEBO_BINARY)
    
    # Show status only
    if args.status:
        manager.print_status()
        sys.exit(0)
    
    # Retry failed batches
    if args.retry_failed:
        count = manager.resume_failed_batches()
        if count == 0:
            sys.exit(0)
        args.resume = True
    
    # Check if we're resuming
    if args.resume:
        if manager.batches:
            print("🔄 Resuming existing batches...")
        else:
            print("❌ No existing batches found. Need to create new ones.")
            if not all([args.gpu, args.start, args.range_bits, args.address]):
                print("Please provide all parameters for new scan:")
                print("  --gpu ID --start HEX --range BITS --address ADDR")
                sys.exit(1)
            print("Creating new batches...")
            manager.create_batches(args.start, args.range_bits, args.gpu, args.address)
    else:
        # Creating new batches
        if not all([args.gpu, args.start, args.range_bits, args.address]):
            print("❌ Missing required parameters for new scan")
            print("Please provide: --gpu ID --start HEX --range BITS --address ADDR")
            sys.exit(1)
        
        print("🆕 Creating new batches...")
        success = manager.create_batches(args.start, args.range_bits, args.gpu, args.address)
        if not success:
            sys.exit(1)
    
    # Start monitor in background thread if requested
    monitor_thread = None
    if args.monitor:
        monitor_thread = threading.Thread(target=manager.monitor_progress, daemon=True)
        monitor_thread.start()
        # Give monitor time to start
        time.sleep(1)
    
    # Run batches
    try:
        manager.run_batches()
    except KeyboardInterrupt:
        print("\n\n⚠️  Batch processing interrupted")
    finally:
        print("\n✅ Batch manager finished")

if __name__ == "__main__":
    main()
