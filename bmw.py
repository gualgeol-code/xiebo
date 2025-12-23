import subprocess
import sys
import os
import time
import math
from datetime import datetime
import csv

# Konfigurasi file log
LOG_FILE = "logbatch.txt"
DRIVE_MOUNT_PATH = "/content/drive"
DRIVE_FILE_PATH = "/content/drive/MyDrive/logbatch.txt"

# Kolom-kolom untuk tabel log
LOG_COLUMNS = [
    'batch_id',
    'start_hex',
    'range_bits',
    'total_keys',
    'address_target',
    'gpu_id',
    'status',
    'found',
    'wif_key',
    'address_found',
    'private_key',
    'return_code',
    'created_at',
    'updated_at',
    'started_at',
    'completed_at',
    'error_message'
]

def save_to_drive():
    """Menyimpan logbatch.txt ke Google Drive"""
    try:
        # Cek apakah Google Drive tersedia (untuk Google Colab)
        if os.path.exists(DRIVE_MOUNT_PATH):
            from google.colab import drive
            import shutil
            
            # Mount drive jika belum
            if not os.path.exists(os.path.join(DRIVE_MOUNT_PATH, "MyDrive")):
                drive.mount(DRIVE_MOUNT_PATH, force_remount=False)
            
            # Salin file
            src = LOG_FILE
            dst = DRIVE_FILE_PATH
            
            shutil.copy(src, dst)
            # HAPUS: Tidak tampilkan pesan save ke drive di terminal
            # print(f"📁 Log saved to Google Drive: {dst}")
        else:
            # HAPUS: Tidak tampilkan pesan
            pass
    except ImportError:
        # HAPUS: Tidak tampilkan pesan
        pass
    except Exception as e:
        # Hanya tampilkan error jika penting
        print(f"⚠️ Failed to save to Google Drive: {e}")

def read_log_as_dict():
    """Membaca log file dan mengembalikan dictionary berdasarkan batch_id"""
    log_dict = {}
    
    if not os.path.exists(LOG_FILE):
        return log_dict
    
    try:
        with open(LOG_FILE, 'r') as f:
            # Baca header
            reader = csv.DictReader(f, delimiter='|')
            for row in reader:
                batch_id = row.get('batch_id', '').strip()
                if batch_id:
                    log_dict[batch_id] = row
    except Exception as e:
        # Hanya tampilkan error jika penting
        print(f"⚠️ Error reading log file: {e}")
    
    return log_dict

def write_log_from_dict(log_dict):
    """Menulis log file dari dictionary"""
    try:
        # Konversi dictionary ke list
        rows = []
        for batch_id in sorted(log_dict.keys(), key=lambda x: int(x) if x.isdigit() else x):
            rows.append(log_dict[batch_id])
        
        # Tulis ke file dengan format tabel
        with open(LOG_FILE, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=LOG_COLUMNS, delimiter='|')
            writer.writeheader()
            writer.writerows(rows)
        
        # HAPUS: Tidak tampilkan pesan update log di terminal
        # print(f"📝 Log file updated with {len(rows)} entries")
        
        # Simpan ke Google Drive (silent)
        save_to_drive()
        
    except Exception as e:
        print(f"❌ Error writing log file: {e}")

def update_batch_log(batch_info):
    """Update log batch dengan informasi status terbaru"""
    try:
        # Baca log yang sudah ada
        log_dict = read_log_as_dict()
        
        # Pastikan batch_info memiliki semua kolom yang diperlukan
        for column in LOG_COLUMNS:
            if column not in batch_info:
                batch_info[column] = ''
        
        # Update atau tambah entry
        batch_id = str(batch_info.get('batch_id', ''))
        log_dict[batch_id] = batch_info
        
        # Tulis kembali log (silent)
        write_log_from_dict(log_dict)
        
        # HAPUS: Tidak tampilkan pesan update log di terminal
        # print(f"📝 Log updated: Batch {batch_id} - {batch_info.get('status', 'N/A')}")
        
    except Exception as e:
        print(f"❌ Error updating log: {e}")

def parse_xiebo_output(output_text):
    """Parse output dari xiebo untuk mencari private key yang ditemukan"""
    found_info = {
        'found': False,
        'wif_key': '',
        'address': '',
        'private_key': ''
    }
    
    lines = output_text.split('\n')
    for line in lines:
        line_lower = line.lower()
        if 'found' in line_lower and 'private' in line_lower:
            found_info['found'] = True
            
            # Coba ekstrak informasi
            if 'wif' in line_lower:
                parts = line.split(':')
                if len(parts) > 1:
                    found_info['wif_key'] = parts[1].strip()
            
            if 'address' in line_lower:
                parts = line.split(':')
                if len(parts) > 1:
                    found_info['address'] = parts[1].strip()
            
            # Simpan line lengkap sebagai private_key jika format tidak standar
            if not found_info['wif_key'] and not found_info['address']:
                found_info['private_key'] = line.strip()
    
    return found_info

def run_xiebo(gpu_id, start_hex, range_bits, address, batch_id=None):
    """Run xiebo binary directly"""
    cmd = ["./xiebo", "-gpuId", str(gpu_id), "-start", start_hex, 
           "-range", str(range_bits), address]
    
    print(f"\n{'='*60}")
    print(f"Running: {' '.join(cmd)}")
    print(f"{'='*60}")
    
    try:
        # Update status menjadi inprogress jika ada batch_id
        if batch_id is not None:
            batch_info = {
                'batch_id': str(batch_id),
                'start_hex': start_hex,
                'range_bits': str(range_bits),
                'total_keys': '',
                'address_target': address,
                'gpu_id': str(gpu_id),
                'status': 'inprogress',
                'found': '',
                'wif_key': '',
                'address_found': '',
                'private_key': '',
                'return_code': '',
                'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'started_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'completed_at': '',
                'error_message': ''
            }
            update_batch_log(batch_info)
        
        # Jalankan xiebo dan tangkap output
        result = subprocess.run(cmd, capture_output=True, text=True)
        output_text = result.stdout + result.stderr
        
        # Parse output untuk mencari private key
        found_info = parse_xiebo_output(output_text)
        
        # Update status berdasarkan hasil
        if batch_id is not None:
            batch_info['status'] = 'done'
            batch_info['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            batch_info['completed_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            batch_info['found'] = 'YES' if found_info['found'] else 'NO'
            batch_info['return_code'] = str(result.returncode)
            
            if found_info['found']:
                batch_info['wif_key'] = found_info['wif_key'] if found_info['wif_key'] else 'N/A'
                batch_info['address_found'] = found_info['address'] if found_info['address'] else 'N/A'
                batch_info['private_key'] = found_info['private_key'] if found_info['private_key'] else 'N/A'
                
                print(f"🎉 PRIVATE KEY FOUND!")
                if found_info['wif_key']:
                    print(f"   WIF Key: {found_info['wif_key']}")
                if found_info['address']:
                    print(f"   Address: {found_info['address']}")
            else:
                batch_info['wif_key'] = ''
                batch_info['address_found'] = ''
                batch_info['private_key'] = ''
                print(f"🔍 Private key not found in this batch")
            
            update_batch_log(batch_info)
        
        return result.returncode, found_info
        
    except KeyboardInterrupt:
        print("\n⚠️ Stopped by user")
        
        # Update status jika batch diinterupsi
        if batch_id is not None:
            batch_info = {
                'batch_id': str(batch_id),
                'start_hex': start_hex,
                'range_bits': str(range_bits),
                'total_keys': '',
                'address_target': address,
                'gpu_id': str(gpu_id),
                'status': 'interrupted',
                'found': '',
                'wif_key': '',
                'address_found': '',
                'private_key': '',
                'return_code': '130',
                'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'started_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'completed_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'error_message': 'Stopped by user'
            }
            update_batch_log(batch_info)
        
        return 130, {'found': False}
    except Exception as e:
        error_msg = str(e)
        print(f"❌ Error: {error_msg}")
        
        # Update status error jika ada batch_id
        if batch_id is not None:
            batch_info = {
                'batch_id': str(batch_id),
                'start_hex': start_hex,
                'range_bits': str(range_bits),
                'total_keys': '',
                'address_target': address,
                'gpu_id': str(gpu_id),
                'status': 'error',
                'found': '',
                'wif_key': '',
                'address_found': '',
                'private_key': '',
                'return_code': '1',
                'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'started_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'completed_at': '',
                'error_message': error_msg
            }
            update_batch_log(batch_info)
        
        return 1, {'found': False}

def initialize_batch_log(start_hex, range_bits, address, gpu_id, num_batches, batch_size):
    """Inisialisasi log batch dengan semua batch dalam status uncheck"""
    log_dict = read_log_as_dict()
    
    start_int = int(start_hex, 16)
    total_keys = 1 << range_bits
    end_int = start_int + total_keys - 1
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    for i in range(num_batches):
        batch_start = start_int + (i * batch_size)
        batch_end = min(batch_start + batch_size, end_int + 1)
        batch_keys = batch_end - batch_start
        
        if batch_keys <= 1:
            batch_bits = 1
        else:
            batch_bits = math.ceil(math.log2(batch_keys))
        
        batch_hex = format(batch_start, 'x')
        
        batch_info = {
            'batch_id': str(i),
            'start_hex': batch_hex,
            'range_bits': str(batch_bits),
            'total_keys': f"{batch_keys:,}",
            'address_target': address,
            'gpu_id': str(gpu_id),
            'status': 'uncheck',
            'found': '',
            'wif_key': '',
            'address_found': '',
            'private_key': '',
            'return_code': '',
            'created_at': current_time,
            'updated_at': current_time,
            'started_at': '',
            'completed_at': '',
            'error_message': ''
        }
        
        # Hanya tambah jika belum ada
        if str(i) not in log_dict:
            log_dict[str(i)] = batch_info
    
    # Tulis ke file (silent)
    write_log_from_dict(log_dict)
    
    # HAPUS: Tidak tampilkan pesan inisialisasi log
    # print(f"📋 Initialized {num_batches} batch entries in {LOG_FILE}")
    # print(f"📊 Log format: CSV with '|' delimiter")
    
    return log_dict

def get_log_summary():
    """Mendapatkan summary log tanpa menampilkan isi file"""
    if not os.path.exists(LOG_FILE):
        return None, None, None
    
    try:
        with open(LOG_FILE, 'r') as f:
            lines = f.readlines()
        
        if len(lines) <= 1:  # Hanya header
            return 0, 0, {}
        
        # Hitung status
        status_counts = {}
        found_count = 0
        total_batches = 0
        
        # Baca data (skip header)
        reader = csv.DictReader(lines, delimiter='|')
        for row in reader:
            total_batches += 1
            status = row.get('status', 'unknown')
            status_counts[status] = status_counts.get(status, 0) + 1
            
            if row.get('found') == 'YES':
                found_count += 1
        
        return total_batches, found_count, status_counts
        
    except Exception as e:
        print(f"⚠️  Error getting log summary: {e}")
        return None, None, None

def display_compact_summary():
    """Menampilkan summary yang ringkas tanpa isi log"""
    total_batches, found_count, status_counts = get_log_summary()
    
    if total_batches is None:
        print("📭 No log file found")
        return
    
    print(f"\n{'='*50}")
    print("📊 LOG SUMMARY (Compact)")
    print(f"{'='*50}")
    print(f"Total batches: {total_batches}")
    print(f"Found private keys: {found_count}")
    
    if status_counts:
        print("\nStatus distribution:")
        for status, count in sorted(status_counts.items()):
            percentage = (count / total_batches) * 100
            print(f"  {status:<12}: {count:>4} ({percentage:>5.1f}%)")
    
    print(f"{'='*50}")

def main():
    # Parse arguments directly
    if len(sys.argv) < 2:
        print("Xiebo Batch Runner")
        print("Usage:")
        print("  Single run: python3 xiebo_runner_fixed.py GPU_ID START_HEX RANGE_BITS ADDRESS")
        print("  Batch run:  python3 xiebo_runner_fixed.py --batch GPU_ID START_HEX RANGE_BITS ADDRESS")
        print("  Show summary: python3 xiebo_runner_fixed.py --summary")
        sys.exit(1)
    
    # Show summary mode
    if sys.argv[1] == "--summary":
        display_compact_summary()
        sys.exit(0)
    
    # Single run mode
    if len(sys.argv) == 5:
        gpu_id = sys.argv[1]
        start_hex = sys.argv[2]
        range_bits = int(sys.argv[3])
        address = sys.argv[4]
        
        print(f"Single run mode")
        return_code, found_info = run_xiebo(gpu_id, start_hex, range_bits, address)
        
        if found_info['found']:
            print("\n🎯 RESULT: PRIVATE KEY FOUND!")
            if found_info['wif_key']:
                print(f"   WIF Key: {found_info['wif_key']}")
            if found_info['address']:
                print(f"   Address: {found_info['address']}")
            if found_info['private_key']:
                print(f"   Private Key: {found_info['private_key']}")
        
        return return_code
    
    # Batch run mode
    elif sys.argv[1] == "--batch" and len(sys.argv) == 6:
        gpu_id = sys.argv[2]
        start_hex = sys.argv[3]
        range_bits = int(sys.argv[4])
        address = sys.argv[5]
        
        BATCH_SIZE = 100000000000  # 100 M,10 T keys per batch 10000000000000
        
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
        print(f"Log file: {LOG_FILE} (auto-saved to Google Drive)")
        print(f"{'='*60}")
        
        # Calculate batches
        num_batches = math.ceil(total_keys / BATCH_SIZE)
        
        print(f"\nNumber of batches: {num_batches}")
        print("First 3 batches:")
        
        # Inisialisasi log batch (silent)
        initialize_batch_log(start_hex, range_bits, address, gpu_id, num_batches, BATCH_SIZE)
        
        # Tampilkan batch pertama saja
        for i in range(min(3, num_batches)):
            batch_start = start_int + (i * BATCH_SIZE)
            batch_end = min(batch_start + BATCH_SIZE, end_int + 1)
            batch_keys = batch_end - batch_start
            
            if batch_keys <= 1:
                batch_bits = 1
            else:
                batch_bits = math.ceil(math.log2(batch_keys))
            
            batch_hex = format(batch_start, 'x')
            print(f"  Batch {i}: 0x{batch_hex} [{batch_bits} bits, {batch_keys:,} keys]")
        
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
            
            # Run this batch
            print(f"\n▶️  Starting batch {i+1}/{num_batches}")
            print(f"   Start: 0x{batch_hex}")
            print(f"   Bits: {batch_bits}")
            print(f"   Keys: {batch_keys:,}")
            
            return_code, found_info = run_xiebo(gpu_id, batch_hex, batch_bits, address, batch_id=i)
            
            if return_code == 0:
                print(f"✅ Batch {i+1} completed")
            else:
                print(f"⚠️  Batch {i+1} exited with code {return_code}")
            
            # Tampilkan progress setiap 10 batch atau batch terakhir
            if (i + 1) % 10 == 0 or i == num_batches - 1:
                total_processed = min((i + 1) * BATCH_SIZE, total_keys)
                percentage = (total_processed / total_keys) * 100
                print(f"\n📈 Progress: {i+1}/{num_batches} batches ({percentage:.1f}%)")
            
            # Delay between batches (except last one)
            if i < num_batches - 1:
                print(f"⏱️  Waiting 5 seconds...")
                time.sleep(5)
        
        print(f"\n{'='*60}")
        print(f"✅ ALL BATCHES COMPLETED!")
        
        # Tampilkan summary ringkas
        display_compact_summary()
        
        # Cek jika ada private key yang ditemukan
        total_batches, found_count, _ = get_log_summary()
        if found_count and found_count > 0:
            print(f"\n🎯 {found_count} PRIVATE KEY(S) FOUND!")
            print(f"   Check {LOG_FILE} for details")
            print(f"   File also saved to Google Drive")
        
        print(f"{'='*60}")
        
    else:
        print("Invalid arguments")
        print("Usage: python3 xiebo_runner_fixed.py GPU_ID START_HEX RANGE_BITS ADDRESS")
        print("Or:    python3 xiebo_runner_fixed.py --batch GPU_ID START_HEX RANGE_BITS ADDRESS")
        print("Or:    python3 xiebo_runner_fixed.py --summary")
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
