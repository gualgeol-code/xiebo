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
    'address_target',
    'status',
    'found',
    'wif'  # Kolom baru untuk menyimpan WIF key
]

# Global flag untuk menghentikan pencarian
STOP_SEARCH_FLAG = False

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
        else:
            pass
    except ImportError:
        pass
    except Exception as e:
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
        
    except Exception as e:
        print(f"❌ Error updating log: {e}")

def parse_xiebo_output(output_text):
    """Parse output dari xiebo untuk mencari private key yang ditemukan"""
    global STOP_SEARCH_FLAG
    
    found_info = {
        'found': False,
        'found_count': 0,  # Jumlah yang ditemukan dari "Found: X"
        'wif_key': '',
        'address': '',
        'private_key_hex': '',
        'private_key_wif': '',
        'raw_output': '',
        'speed_info': ''
    }
    
    lines = output_text.split('\n')
    found_lines = []
    
    for line in lines:
        line_stripped = line.strip()
        line_lower = line_stripped.lower()
        
        # 1. Cari pattern "Found: X" di baris "Range Finished!"
        if 'range finished!' in line_lower and 'found:' in line_lower:
            # Ekstrak angka setelah "Found:"
            import re
            found_match = re.search(r'found:\s*(\d+)', line_lower)
            if found_match:
                found_count = int(found_match.group(1))
                found_info['found_count'] = found_count
                found_info['found'] = found_count > 0
                found_info['speed_info'] = line_stripped
                found_lines.append(line_stripped)
                
                # ⭐ PERUBAHAN UTAMA: Set flag berhenti jika ditemukan 1 atau lebih
                if found_count >= 1:
                    STOP_SEARCH_FLAG = True
                    print(f"🚨 STOP_SEARCH_FLAG diaktifkan karena Found: {found_count}")
        
        # 2. Cari pattern Priv (HEX):
        elif 'priv (hex):' in line_lower:
            found_info['found'] = True
            found_info['private_key_hex'] = line_stripped.replace('Priv (HEX):', '').replace('Priv (hex):', '').strip()
            found_lines.append(line_stripped)
        
        # 3. Cari pattern Priv (WIF):
        elif 'priv (wif):' in line_lower:
            found_info['found'] = True
            wif_value = line_stripped.replace('Priv (WIF):', '').replace('Priv (wif):', '').strip()
            found_info['private_key_wif'] = wif_value
            
            # Ambil 60 karakter pertama dari WIF key
            if len(wif_value) >= 60:
                found_info['wif_key'] = wif_value[:60]
            else:
                found_info['wif_key'] = wif_value
                
            found_lines.append(line_stripped)
        
        # 4. Cari pattern Address:
        elif 'address:' in line_lower and found_info['found']:
            found_info['address'] = line_stripped.replace('Address:', '').replace('address:', '').strip()
            found_lines.append(line_stripped)
        
        # 5. Cari pattern "Found" atau "Success" lainnya
        elif any(keyword in line_lower for keyword in ['found', 'success', 'match']) and 'private' in line_lower:
            found_info['found'] = True
            found_lines.append(line_stripped)
    
    # Gabungkan semua line yang ditemukan
    if found_lines:
        found_info['raw_output'] = '\n'.join(found_lines)
        
        # Jika WIF key ditemukan, pastikan wif_key terisi
        if found_info['private_key_wif'] and not found_info['wif_key']:
            wif_value = found_info['private_key_wif']
            if len(wif_value) >= 60:
                found_info['wif_key'] = wif_value[:60]
            else:
                found_info['wif_key'] = wif_value
        # Jika HEX ditemukan tapi WIF tidak, gunakan HEX sebagai private_key
        elif found_info['private_key_hex'] and not found_info['wif_key']:
            found_info['wif_key'] = found_info['private_key_hex'][:60] if len(found_info['private_key_hex']) >= 60 else found_info['private_key_hex']
    
    return found_info

def run_xiebo(gpu_id, start_hex, range_bits, address, batch_id=None):
    """Run xiebo binary langsung dan tampilkan outputnya"""
    global STOP_SEARCH_FLAG
    
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
                'address_target': address,
                'status': 'inprogress',
                'found': '',
                'wif': ''
            }
            update_batch_log(batch_info)
        
        # Jalankan xiebo dan tampilkan output secara real-time
        print(f"\n📤 Starting xiebo process...\n")
        print(f"{'-'*60}")
        
        # Gunakan Popen untuk mendapatkan output real-time
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        # Tampilkan output secara real-time
        output_lines = []
        while True:
            output_line = process.stdout.readline()
            if output_line == '' and process.poll() is not None:
                break
            if output_line:
                # Tampilkan output dengan format yang lebih baik
                stripped_line = output_line.strip()
                if stripped_line:
                    print(f"   {stripped_line}")
                output_lines.append(output_line)
        
        # Tunggu proses selesai
        return_code = process.wait()
        output_text = ''.join(output_lines)
        
        # Parse output untuk mencari private key
        found_info = parse_xiebo_output(output_text)
        
        # Update status berdasarkan hasil
        if batch_id is not None:
            batch_info['status'] = 'done'
            
            # Tentukan nilai 'found' berdasarkan found_count atau found status
            if found_info['found_count'] > 0:
                batch_info['found'] = 'YES'
            elif found_info['found']:
                batch_info['found'] = 'YES'
            else:
                batch_info['found'] = 'NO'
            
            # Simpan WIF key ke kolom wif (60 karakter pertama)
            if found_info['wif_key']:
                batch_info['wif'] = found_info['wif_key'][:60]
            else:
                batch_info['wif'] = ''
                
            update_batch_log(batch_info)
        
        # Tampilkan hasil pencarian
        print(f"\n{'='*60}")
        print(f"🔍 SEARCH RESULT")
        print(f"{'='*60}")
        
        if found_info['found_count'] > 0:
            print(f"✅ FOUND: {found_info['found_count']} PRIVATE KEY(S)!")
        elif found_info['found']:
            print(f"✅ PRIVATE KEY FOUND!")
        else:
            print(f"❌ Private key not found in this batch")
        
        if found_info['speed_info']:
            print(f"\n📊 {found_info['speed_info']}")
        
        if found_info['found'] or found_info['found_count'] > 0:
            print(f"\n📋 Found information:")
            if found_info['raw_output']:
                for line in found_info['raw_output'].split('\n'):
                    print(f"   {line}")
            else:
                if found_info['private_key_hex']:
                    print(f"   Priv (HEX): {found_info['private_key_hex']}")
                if found_info['private_key_wif']:
                    print(f"   Priv (WIF): {found_info['private_key_wif']}")
                if found_info['address']:
                    print(f"   Address: {found_info['address']}")
                if found_info['wif_key']:
                    print(f"   WIF Key (first 60 chars): {found_info['wif_key']}")
        
        print(f"{'='*60}")
        
        return return_code, found_info
        
    except KeyboardInterrupt:
        print("\n\n⚠️ Stopped by user")
        
        # Update status jika batch diinterupsi
        if batch_id is not None:
            batch_info = {
                'batch_id': str(batch_id),
                'start_hex': start_hex,
                'range_bits': str(range_bits),
                'address_target': address,
                'status': 'interrupted',
                'found': '',
                'wif': ''
            }
            update_batch_log(batch_info)
        
        return 130, {'found': False}
    except Exception as e:
        error_msg = str(e)
        print(f"\n❌ Error: {error_msg}")
        
        # Update status error jika ada batch_id
        if batch_id is not None:
            batch_info = {
                'batch_id': str(batch_id),
                'start_hex': start_hex,
                'range_bits': str(range_bits),
                'address_target': address,
                'status': 'error',
                'found': '',
                'wif': ''
            }
            update_batch_log(batch_info)
        
        return 1, {'found': False}

def initialize_batch_log(start_hex, range_bits, address, gpu_id, num_batches, batch_size):
    """Inisialisasi log batch dengan semua batch dalam status uncheck"""
    log_dict = read_log_as_dict()
    
    start_int = int(start_hex, 16)
    total_keys = 1 << range_bits
    end_int = start_int + total_keys - 1
    
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
            'address_target': address,
            'status': 'uncheck',
            'found': '',
            'wif': ''
        }
        
        # Hanya tambah jika belum ada
        if str(i) not in log_dict:
            log_dict[str(i)] = batch_info
    
    # Tulis ke file (silent)
    write_log_from_dict(log_dict)
    
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
    print("📊 LOG SUMMARY")
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
    global STOP_SEARCH_FLAG
    
    # Reset flag stop search setiap kali program dijalankan
    STOP_SEARCH_FLAG = False
    
    # Parse arguments directly
    if len(sys.argv) < 2:
        print("Xiebo Batch Runner with Auto-Stop Feature")
        print("Usage:")
        print("  Single run: python3 xiebo_runner_fixed.py GPU_ID START_HEX RANGE_BITS ADDRESS")
        print("  Batch run:  python3 xiebo_runner_fixed.py --batch GPU_ID START_HEX RANGE_BITS ADDRESS")
        print("  Show summary: python3 xiebo_runner_fixed.py --summary")
        print("\n⚠️  FEATURE: Auto-stop ketika ditemukan Found: 1 atau lebih")
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
        
        print(f"\n{'='*60}")
        print(f"SINGLE RUN MODE")
        print(f"{'='*60}")
        print(f"GPU: {gpu_id}")
        print(f"Start: 0x{start_hex}")
        print(f"Range: {range_bits} bits")
        print(f"Address: {address}")
        print(f"{'='*60}")
        
        return_code, found_info = run_xiebo(gpu_id, start_hex, range_bits, address)
        
        return return_code
    
    # Batch run mode
    elif sys.argv[1] == "--batch" and len(sys.argv) == 6:
        gpu_id = sys.argv[2]
        start_hex = sys.argv[3]
        range_bits = int(sys.argv[4])
        address = sys.argv[5]
        
        BATCH_SIZE = 1000000000000  # 100 M,10 T keys per batch 10000000000000
        
        # Calculate total
        start_int = int(start_hex, 16)
        total_keys = 1 << range_bits
        end_int = start_int + total_keys - 1
        
        print(f"\n{'='*60}")
        print(f"BATCH MODE (with AUTO-STOP when Found: 1+)")
        print(f"{'='*60}")
        print(f"GPU: {gpu_id}")
        print(f"Start: 0x{start_hex}")
        print(f"Range: {range_bits} bits")
        print(f"Total keys: {total_keys:,}")
        print(f"End: 0x{format(end_int, 'x')}")
        print(f"Batch size: {BATCH_SIZE:,} keys")
        print(f"Address: {address}")
        print(f"Log file: {LOG_FILE} (7 columns, auto-saved to Google Drive)")
        print(f"⚠️  AUTO-STOP: Pencarian akan berhenti otomatis jika ditemukan Found: 1 atau lebih")
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
            # ⭐ PERUBAHAN UTAMA: Cek flag stop search sebelum menjalankan batch
            if STOP_SEARCH_FLAG:
                print(f"\n{'='*60}")
                print(f"🚨 AUTO-STOP TRIGGERED!")
                print(f"{'='*60}")
                print(f"Pencarian dihentikan karena private key telah ditemukan")
                print(f"Batch yang tersisa ({i+1}/{num_batches}) tidak akan dijalankan")
                print(f"{'='*60}")
                break
            
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
            print(f"\n{'='*60}")
            print(f"▶️  BATCH {i+1}/{num_batches}")
            print(f"{'='*60}")
            print(f"Start: 0x{batch_hex}")
            print(f"Bits: {batch_bits}")
            print(f"Keys: {batch_keys:,}")
            
            return_code, found_info = run_xiebo(gpu_id, batch_hex, batch_bits, address, batch_id=i)
            
            if return_code == 0:
                print(f"✅ Batch {i+1} completed successfully")
            else:
                print(f"⚠️  Batch {i+1} exited with code {return_code}")
            
            # Tampilkan progress setiap 10 batch atau batch terakhir
            if (i + 1) % 10 == 0 or i == num_batches - 1:
                total_processed = min((i + 1) * BATCH_SIZE, total_keys)
                percentage = (total_processed / total_keys) * 100
                print(f"\n📈 Overall Progress: {i+1}/{num_batches} batches ({percentage:.1f}%)")
            
            # ⭐ PERUBAHAN UTAMA: Delay antara batch hanya jika tidak ada flag stop
            if i < num_batches - 1 and not STOP_SEARCH_FLAG:
                print(f"\n⏱️  Waiting 5 seconds before next batch...")
                time.sleep(5)
        
        print(f"\n{'='*60}")
        if STOP_SEARCH_FLAG:
            print(f"🎯 SEARCH STOPPED - PRIVATE KEY FOUND!")
        else:
            print(f"🎉 ALL BATCHES COMPLETED!")
        print(f"{'='*60}")
        
        # Tampilkan summary ringkas
        display_compact_summary()
        
        # Cek jika ada private key yang ditemukan
        total_batches, found_count, _ = get_log_summary()
        if found_count and found_count > 0:
            print(f"\n🔥 {found_count} PRIVATE KEY(S) FOUND!")
            print(f"   Check {LOG_FILE} for batch details and WIF keys")
            print(f"   File also auto-saved to Google Drive")
        
        # Tampilkan isi log file terakhir
        print(f"\n📄 Final log file content ({LOG_FILE}):")
        print(f"{'='*60}")
        if os.path.exists(LOG_FILE):
            with open(LOG_FILE, 'r') as f:
                content = f.read()
                print(content)
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
