import subprocess
import sys
import os
import time
import math
from datetime import datetime
import csv
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# Konfigurasi file log
LOG_FILE = "logbatch.txt"
NEXT_BATCH_FILE = "nextbatch.txt"  # File untuk menyimpan start range berikutnya
DRIVE_MOUNT_PATH = "/content/drive"
DRIVE_FILE_PATH = "/content/drive/MyDrive/logbatch.txt"
DRIVE_NEXT_BATCH_PATH = "/content/drive/MyDrive/nextbatch.txt"

# Kolom-kolom untuk tabel log (ditambah kolom state)
LOG_COLUMNS = [
    'batch_id',
    'start_hex',
    'range_bits',
    'address_target',
    'status',
    'found',
    'wif',
    'state_info',
    'gpu_id'  # Kolom baru untuk melacak GPU yang digunakan
]

# Konfigurasi batch
MAX_BATCHES_PER_RUN = 1000000  # Maksimal 1juta batch per eksekusi
BATCH_SIZE = 2000000000000  # 2 triliun keys per batch

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
            
            # Salin file logbatch.txt
            src = LOG_FILE
            dst = DRIVE_FILE_PATH
            shutil.copy(src, dst)
            
            # Salin file nextbatch.txt jika ada
            if os.path.exists(NEXT_BATCH_FILE):
                src_next = NEXT_BATCH_FILE
                dst_next = DRIVE_NEXT_BATCH_PATH
                shutil.copy(src_next, dst_next)
                
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

def save_next_batch_info(start_hex, range_bits, address, next_start_hex, batches_completed, total_batches, gpu_ids=None, timestamp=None):
    """Menyimpan informasi batch berikutnya ke file dan log"""
    try:
        if timestamp is None:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        info = {
            'original_start': start_hex,
            'original_range_bits': str(range_bits),
            'address': address,
            'next_start_hex': next_start_hex,
            'batches_completed': str(batches_completed),
            'total_batches': str(total_batches),
            'gpu_ids': ','.join(map(str, gpu_ids)) if gpu_ids else '0',
            'timestamp': timestamp
        }
        
        # 1. Simpan ke file nextbatch.txt
        with open(NEXT_BATCH_FILE, 'w') as f:
            for key, value in info.items():
                f.write(f"{key}={value}\n")
        
        # 2. Tambahkan ke logbatch.txt sebagai entri khusus
        log_dict = read_log_as_dict()
        
        gpu_info = f"gpus={info['gpu_ids']}" if gpu_ids else ""
        state_info = f"NEXT_BATCH|next_start={next_start_hex}|completed={batches_completed}|total={total_batches}|{gpu_info}|time={timestamp}"
        
        # Buat entry khusus untuk next batch info
        state_entry = {
            'batch_id': 'STATE_INFO',
            'start_hex': next_start_hex,
            'range_bits': str(range_bits),
            'address_target': address,
            'status': 'state_saved',
            'found': '',
            'wif': '',
            'state_info': state_info,
            'gpu_id': info['gpu_ids']
        }
        
        # Tambahkan atau update entry STATE_INFO
        log_dict['STATE_INFO'] = state_entry
        
        # Tulis kembali log
        write_log_from_dict(log_dict)
        
        # Simpan ke Google Drive
        save_to_drive()
        
        print(f"📝 Next batch info saved:")
        print(f"   File: {NEXT_BATCH_FILE}")
        print(f"   Log: {LOG_FILE} (as STATE_INFO entry)")
        print(f"   Next start: 0x{next_start_hex}")
        print(f"   Progress: {batches_completed}/{total_batches} batches")
        if gpu_ids:
            print(f"   GPU IDs: {gpu_ids}")
        
    except Exception as e:
        print(f"❌ Error saving next batch info: {e}")

def load_next_batch_info():
    """Memuat informasi batch berikutnya dari file"""
    if not os.path.exists(NEXT_BATCH_FILE):
        # Coba baca dari log file jika nextbatch.txt tidak ada
        return load_next_batch_from_log()
    
    try:
        info = {}
        with open(NEXT_BATCH_FILE, 'r') as f:
            for line in f:
                line = line.strip()
                if '=' in line:
                    key, value = line.split('=', 1)
                    info[key] = value
        
        # Parse GPU IDs jika ada
        if 'gpu_ids' in info:
            gpu_ids_str = info['gpu_ids']
            if gpu_ids_str:
                gpu_ids = list(map(int, gpu_ids_str.split(',')))
                info['gpu_ids'] = gpu_ids
            else:
                info['gpu_ids'] = [0]
        else:
            info['gpu_ids'] = [0]
            
        return info
    except Exception as e:
        print(f"❌ Error loading next batch info from file: {e}")
        return load_next_batch_from_log()

def load_next_batch_from_log():
    """Memuat informasi next batch dari log file"""
    try:
        log_dict = read_log_as_dict()
        
        # Cari entry STATE_INFO
        if 'STATE_INFO' in log_dict:
            state_info = log_dict['STATE_INFO'].get('state_info', '')
            
            # Parse state_info
            info = {}
            gpu_ids = []
            parts = state_info.split('|')
            for part in parts:
                if '=' in part:
                    key, value = part.split('=', 1)
                    if key == 'next_start':
                        info['next_start_hex'] = value
                    elif key == 'completed':
                        info['batches_completed'] = value
                    elif key == 'total':
                        info['total_batches'] = value
                    elif key == 'gpus':
                        gpu_ids = list(map(int, value.split(',')))
                    elif key == 'time':
                        info['timestamp'] = value
            
            # Ambil info lainnya dari entry
            info['address'] = log_dict['STATE_INFO'].get('address_target', '')
            info['original_start'] = ''  # Tidak disimpan di log
            info['original_range_bits'] = log_dict['STATE_INFO'].get('range_bits', '')
            info['gpu_ids'] = gpu_ids if gpu_ids else [0]
            
            print(f"📋 Loaded next batch info from log file")
            return info
        
        return None
    except Exception as e:
        print(f"❌ Error loading next batch info from log: {e}")
        return None

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
                
                # Set flag berhenti jika ditemukan 1 atau lebih
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

def run_xiebo_single_batch(gpu_id, start_hex, range_bits, address, batch_id=None):
    """Run xiebo binary untuk single GPU dengan batch tertentu"""
    global STOP_SEARCH_FLAG
    
    cmd = ["./xiebo", "-gpuId", str(gpu_id), "-start", start_hex, 
           "-range", str(range_bits), address]
    
    print(f"\n{'='*60}")
    print(f"GPU {gpu_id} Batch {batch_id if batch_id is not None else 'N/A'}: Running {' '.join(cmd)}")
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
                'wif': '',
                'state_info': '',
                'gpu_id': str(gpu_id)
            }
            update_batch_log(batch_info)
        
        # Jalankan xiebo dan tampilkan output secara real-time
        print(f"\n📤 GPU {gpu_id} Batch {batch_id if batch_id is not None else 'N/A'}: Starting xiebo process...\n")
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
        
        # Tampilkan output secara real-time dengan prefiks GPU
        output_lines = []
        while True:
            output_line = process.stdout.readline()
            if output_line == '' and process.poll() is not None:
                break
            if output_line:
                # Tampilkan output dengan format yang lebih baik
                stripped_line = output_line.strip()
                if stripped_line:
                    print(f"   GPU {gpu_id}: {stripped_line}")
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
        print(f"🔍 GPU {gpu_id} Batch {batch_id if batch_id is not None else 'N/A'}: SEARCH RESULT")
        print(f"{'='*60}")
        
        if found_info['found_count'] > 0:
            print(f"✅ GPU {gpu_id}: FOUND {found_info['found_count']} PRIVATE KEY(S)!")
        elif found_info['found']:
            print(f"✅ GPU {gpu_id}: PRIVATE KEY FOUND!")
        else:
            print(f"❌ GPU {gpu_id}: Private key not found in this batch")
        
        if found_info['speed_info']:
            print(f"\n📊 GPU {gpu_id}: {found_info['speed_info']}")
        
        if found_info['found'] or found_info['found_count'] > 0:
            print(f"\n📋 GPU {gpu_id}: Found information:")
            if found_info['raw_output']:
                for line in found_info['raw_output'].split('\n'):
                    print(f"   GPU {gpu_id}: {line}")
            else:
                if found_info['private_key_hex']:
                    print(f"   GPU {gpu_id}: Priv (HEX): {found_info['private_key_hex']}")
                if found_info['private_key_wif']:
                    print(f"   GPU {gpu_id}: Priv (WIF): {found_info['private_key_wif']}")
                if found_info['address']:
                    print(f"   GPU {gpu_id}: Address: {found_info['address']}")
                if found_info['wif_key']:
                    print(f"   GPU {gpu_id}: WIF Key (first 60 chars): {found_info['wif_key']}")
        
        print(f"{'='*60}")
        
        return return_code, found_info
        
    except KeyboardInterrupt:
        print(f"\n\n⚠️ GPU {gpu_id} Batch {batch_id if batch_id is not None else 'N/A'}: Stopped by user")
        
        # Update status jika batch diinterupsi
        if batch_id is not None:
            batch_info = {
                'batch_id': str(batch_id),
                'start_hex': start_hex,
                'range_bits': str(range_bits),
                'address_target': address,
                'status': 'interrupted',
                'found': '',
                'wif': '',
                'state_info': '',
                'gpu_id': str(gpu_id)
            }
            update_batch_log(batch_info)
        
        return 130, {'found': False}
    except Exception as e:
        error_msg = str(e)
        print(f"\n❌ GPU {gpu_id} Batch {batch_id if batch_id is not None else 'N/A'}: Error: {error_msg}")
        
        # Update status error jika ada batch_id
        if batch_id is not None:
            batch_info = {
                'batch_id': str(batch_id),
                'start_hex': start_hex,
                'range_bits': str(range_bits),
                'address_target': address,
                'status': 'error',
                'found': '',
                'wif': '',
                'state_info': '',
                'gpu_id': str(gpu_id)
            }
            update_batch_log(batch_info)
        
        return 1, {'found': False}

def run_parallel_batches(gpu_ids, batch_infos, address):
    """Menjalankan multiple batch secara paralel di GPU yang berbeda"""
    global STOP_SEARCH_FLAG
    
    results = []
    
    # Gunakan ThreadPoolExecutor untuk menjalankan batch secara paralel
    with ThreadPoolExecutor(max_workers=len(gpu_ids)) as executor:
        # Submit semua batch ke executor
        future_to_batch = {}
        
        for i, batch_info in enumerate(batch_infos):
            if STOP_SEARCH_FLAG:
                print(f"🚨 Skipping remaining batches due to STOP_SEARCH_FLAG")
                break
                
            gpu_id = gpu_ids[i % len(gpu_ids)]  # Round-robin assignment
            batch_start_hex = batch_info['start_hex']
            batch_bits = batch_info['bits']
            batch_id = batch_info['batch_id']
            
            print(f"\n📋 Scheduling Batch {batch_id} on GPU {gpu_id}")
            print(f"   Start: 0x{batch_start_hex}")
            print(f"   Bits: {batch_bits}")
            
            # Submit batch untuk dieksekusi
            future = executor.submit(
                run_xiebo_single_batch,
                gpu_id, batch_start_hex, batch_bits, address, batch_id
            )
            future_to_batch[future] = {
                'gpu_id': gpu_id,
                'batch_id': batch_id,
                'batch_info': batch_info
            }
        
        # Tunggu dan kumpulkan hasil
        for future in as_completed(future_to_batch):
            if STOP_SEARCH_FLAG:
                # Batalkan semua future yang belum selesai
                for f in future_to_batch.keys():
                    if not f.done():
                        f.cancel()
                break
                
            batch_data = future_to_batch[future]
            try:
                return_code, found_info = future.result()
                results.append({
                    'gpu_id': batch_data['gpu_id'],
                    'batch_id': batch_data['batch_id'],
                    'return_code': return_code,
                    'found_info': found_info
                })
                
                # Cek jika ditemukan private key
                if found_info.get('found_count', 0) > 0 or found_info.get('found', False):
                    print(f"\n🚨 PRIVATE KEY FOUND in Batch {batch_data['batch_id']} on GPU {batch_data['gpu_id']}!")
                
            except Exception as e:
                print(f"❌ Error in parallel execution for Batch {batch_data['batch_id']} on GPU {batch_data['gpu_id']}: {e}")
                results.append({
                    'gpu_id': batch_data['gpu_id'],
                    'batch_id': batch_data['batch_id'],
                    'return_code': 1,
                    'found_info': {'found': False, 'error': str(e)}
                })
    
    return results

def calculate_range_bits(keys_count):
    """Fungsi baru: Menghitung range bits yang benar untuk jumlah keys tertentu"""
    if keys_count <= 1:
        return 1
    
    # Hitung log2 dari jumlah keys
    log2_val = math.log2(keys_count)
    
    # Jika hasil log2 adalah bilangan bulat, gunakan nilai tersebut
    # Jika tidak, gunakan floor + 1 (untuk mencakup semua keys)
    if log2_val.is_integer():
        return int(log2_val)
    else:
        return int(math.floor(log2_val)) + 1

def initialize_batch_log(start_hex, range_bits, address, gpu_ids, num_batches, batch_size, start_batch_id=0, save_state_early=True):
    """Inisialisasi log batch dengan semua batch dalam status uncheck"""
    log_dict = read_log_as_dict()
    
    start_int = int(start_hex, 16)
    total_keys = 1 << range_bits
    end_int = start_int + total_keys - 1
    
    total_batches_needed = math.ceil(total_keys / batch_size)
    
    # ⭐ PERUBAHAN UTAMA: Simpan state di awal sebelum menjalankan batch
    if save_state_early and num_batches < total_batches_needed:
        next_start_int = start_int + (num_batches * batch_size)
        next_start_hex = format(next_start_int, 'x')
        
        # Simpan state info di awal
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        save_next_batch_info(
            start_hex,
            range_bits,
            address,
            next_start_hex,
            start_batch_id,  # Belum ada yang diselesaikan
            total_batches_needed,
            gpu_ids,
            timestamp
        )
        print(f"💾 State saved EARLY at initialization")
        print(f"   Next start will be: 0x{next_start_hex}")
        print(f"   After completing {num_batches} batches")
        if gpu_ids:
            print(f"   GPU IDs: {gpu_ids}")
    
    for i in range(start_batch_id, min(start_batch_id + num_batches, total_batches_needed)):
        batch_start = start_int + (i * batch_size)
        batch_end = min(batch_start + batch_size, end_int + 1)
        batch_keys = batch_end - batch_start
        
        # Gunakan fungsi calculate_range_bits yang benar
        batch_bits = calculate_range_bits(batch_keys)
        
        batch_hex = format(batch_start, 'x')
        
        batch_info = {
            'batch_id': str(i),
            'start_hex': batch_hex,
            'range_bits': str(batch_bits),
            'address_target': address,
            'status': 'uncheck',
            'found': '',
            'wif': '',
            'state_info': '',
            'gpu_id': ''  # Akan diisi saat dijalankan
        }
        
        # Hanya tambah jika belum ada
        if str(i) not in log_dict:
            log_dict[str(i)] = batch_info
    
    # Tulis ke file (silent)
    write_log_from_dict(log_dict)
    
    return log_dict, total_batches_needed

def get_log_summary():
    """Mendapatkan summary log tanpa menampilkan isi file"""
    if not os.path.exists(LOG_FILE):
        return None, None, None, None
    
    try:
        with open(LOG_FILE, 'r') as f:
            lines = f.readlines()
        
        if len(lines) <= 1:  # Hanya header
            return 0, 0, {}, None
        
        # Hitung status
        status_counts = {}
        found_count = 0
        total_batches = 0
        state_info = None
        
        # Baca data (skip header)
        reader = csv.DictReader(lines, delimiter='|')
        for row in reader:
            batch_id = row.get('batch_id', '')
            
            if batch_id == 'STATE_INFO':
                state_info = row.get('state_info', '')
                continue
            
            total_batches += 1
            status = row.get('status', 'unknown')
            status_counts[status] = status_counts.get(status, 0) + 1
            
            if row.get('found') == 'YES':
                found_count += 1
        
        return total_batches, found_count, status_counts, state_info
        
    except Exception as e:
        print(f"⚠️  Error getting log summary: {e}")
        return None, None, None, None

def display_compact_summary():
    """Menampilkan summary yang ringkas dengan state info"""
    total_batches, found_count, status_counts, state_info = get_log_summary()
    
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
    
    if state_info:
        print(f"\n💾 SAVED STATE:")
        parts = state_info.split('|')
        for part in parts:
            if '=' in part:
                key, value = part.split('=', 1)
                if key == 'next_start':
                    print(f"  Next start: 0x{value}")
                elif key == 'completed':
                    print(f"  Batches completed: {value}")
                elif key == 'total':
                    print(f"  Total batches needed: {value}")
                elif key == 'gpus':
                    print(f"  GPU IDs: {value}")
                elif key == 'time':
                    print(f"  Saved at: {value}")
    
    print(f"{'='*50}")

def parse_gpu_ids(gpu_str):
    """Parse string GPU IDs menjadi list of integers"""
    if not gpu_str:
        return [0]
    
    gpu_ids = []
    parts = gpu_str.split()
    for part in parts:
        try:
            gpu_ids.append(int(part))
        except ValueError:
            # Jika ada karakter non-numerik, anggap sebagai single GPU
            continue
    
    # Jika tidak ada GPU ID yang valid, gunakan default
    if not gpu_ids:
        gpu_ids = [0]
    
    # Urutkan dan hapus duplikat
    gpu_ids = sorted(list(set(gpu_ids)))
    
    return gpu_ids

def run_xiebo_parallel_mode(gpu_ids, start_hex, range_bits, address, num_batches_to_run, start_batch_id=0):
    """Mode paralel: Setiap GPU memproses batch yang berbeda"""
    global STOP_SEARCH_FLAG, BATCH_SIZE
    
    start_int = int(start_hex, 16)
    total_keys = 1 << range_bits
    end_int = start_int + total_keys - 1
    
    total_batches_needed = math.ceil(total_keys / BATCH_SIZE)
    
    print(f"\n{'='*60}")
    print(f"PARALLEL EXECUTION MODE")
    print(f"{'='*60}")
    print(f"GPU IDs: {gpu_ids}")
    print(f"Total GPUs: {len(gpu_ids)}")
    print(f"Start batch ID: {start_batch_id}")
    print(f"Batches to run: {num_batches_to_run}")
    print(f"Parallel execution: YES (each GPU processes separate batches)")
    print(f"{'='*60}")
    
    # Siapkan batch untuk dieksekusi
    batch_infos = []
    for i in range(num_batches_to_run):
        if STOP_SEARCH_FLAG:
            break
            
        batch_id = start_batch_id + i
        if batch_id >= total_batches_needed:
            break
            
        batch_start = start_int + (batch_id * BATCH_SIZE)
        batch_end = min(batch_start + BATCH_SIZE, end_int + 1)
        batch_keys = batch_end - batch_start
        
        batch_bits = calculate_range_bits(batch_keys)
        batch_hex = format(batch_start, 'x')
        
        batch_infos.append({
            'batch_id': batch_id,
            'start_hex': batch_hex,
            'bits': batch_bits,
            'keys': batch_keys
        })
    
    if not batch_infos:
        print("❌ No batches to run")
        return []
    
    print(f"\n📋 Prepared {len(batch_infos)} batches for parallel execution")
    
    # Jalankan batch secara paralel
    results = run_parallel_batches(gpu_ids, batch_infos, address)
    
    return results

def run_xiebo_sequential_mode(gpu_ids, start_hex, range_bits, address, num_batches_to_run, start_batch_id=0):
    """Mode sequential: Batch dijalankan satu per satu (untuk single GPU atau debugging)"""
    global STOP_SEARCH_FLAG, BATCH_SIZE
    
    start_int = int(start_hex, 16)
    total_keys = 1 << range_bits
    end_int = start_int + total_keys - 1
    
    total_batches_needed = math.ceil(total_keys / BATCH_SIZE)
    
    print(f"\n{'='*60}")
    print(f"SEQUENTIAL EXECUTION MODE")
    print(f"{'='*60}")
    print(f"GPU IDs: {gpu_ids}")
    print(f"Start batch ID: {start_batch_id}")
    print(f"Batches to run: {num_batches_to_run}")
    print(f"Parallel execution: NO (batches run one by one)")
    print(f"{'='*60}")
    
    results = []
    
    for i in range(num_batches_to_run):
        if STOP_SEARCH_FLAG:
            print(f"\n🚨 AUTO-STOP TRIGGERED! Stopping remaining batches")
            break
            
        batch_id = start_batch_id + i
        if batch_id >= total_batches_needed:
            break
            
        batch_start = start_int + (batch_id * BATCH_SIZE)
        batch_end = min(batch_start + BATCH_SIZE, end_int + 1)
        batch_keys = batch_end - batch_start
        
        batch_bits = calculate_range_bits(batch_keys)
        batch_hex = format(batch_start, 'x')
        
        # Gunakan GPU pertama dalam list untuk sequential mode
        gpu_id = gpu_ids[0] if gpu_ids else 0
        
        print(f"\n{'='*60}")
        print(f"▶️  BATCH {batch_id+1}/{total_batches_needed} (Sequential {i+1}/{num_batches_to_run})")
        print(f"{'='*60}")
        print(f"GPU: {gpu_id}")
        print(f"Start: 0x{batch_hex}")
        print(f"Bits: {batch_bits}")
        print(f"Keys: {batch_keys:,}")
        
        return_code, found_info = run_xiebo_single_batch(gpu_id, batch_hex, batch_bits, address, batch_id=batch_id)
        
        results.append({
            'gpu_id': gpu_id,
            'batch_id': batch_id,
            'return_code': return_code,
            'found_info': found_info
        })
        
        if return_code == 0:
            print(f"✅ Batch {batch_id+1} completed successfully")
        else:
            print(f"⚠️  Batch {batch_id+1} exited with code {return_code}")
        
        # Tampilkan progress
        if (i + 1) % 10 == 0 or i == num_batches_to_run - 1:
            completed_now = start_batch_id + i + 1
            percentage = (completed_now / total_batches_needed) * 100
            print(f"\n📈 Overall Progress: {completed_now}/{total_batches_needed} batches ({percentage:.1f}%)")
        
        # Delay antara batch
        if i < num_batches_to_run - 1 and not STOP_SEARCH_FLAG:
            print(f"\n⏱️  Waiting 5 seconds before next batch...")
            time.sleep(5)
    
    return results

def main():
    global STOP_SEARCH_FLAG, BATCH_SIZE, MAX_BATCHES_PER_RUN
    
    # Reset flag stop search setiap kali program dijalankan
    STOP_SEARCH_FLAG = False
    
    # Parse arguments directly
    if len(sys.argv) < 2:
        print("Xiebo Batch Runner with Multi-GPU Parallel Support")
        print("Usage:")
        print("  Single GPU run: python3 xiebo.py GPU_ID START_HEX RANGE_BITS ADDRESS")
        print("  Multi-GPU run:  python3 xiebo.py GPU_IDS START_HEX RANGE_BITS ADDRESS")
        print("  Batch parallel: python3 xiebo.py --parallel GPU_IDS START_HEX RANGE_BITS ADDRESS BATCH_COUNT")
        print("  Batch sequential: python3 xiebo.py --batch GPU_IDS START_HEX RANGE_BITS ADDRESS")
        print("  Show summary:    python3 xiebo.py --summary")
        print("  Continue:        python3 xiebo.py --continue")
        print("\n⚠️  FEATURES:")
        print("  - Multi-GPU parallel: each GPU processes separate batches")
        print("  - Auto-stop ketika ditemukan Found: 1 atau lebih")
        print(f"  - Maksimal {MAX_BATCHES_PER_RUN} batch per eksekusi")
        print(f"  - Batch size: {BATCH_SIZE:,} keys")
        print("  - Simpan state DI AWAL untuk melanjutkan")
        print("  - State info disimpan di logbatch.txt")
        sys.exit(1)
    
    # Show summary mode
    if sys.argv[1] == "--summary":
        display_compact_summary()
        
        # Tampilkan informasi batch berikutnya jika ada
        next_info = load_next_batch_info()
        if next_info:
            print(f"\n{'='*50}")
            print("📋 NEXT BATCH INFO (from nextbatch.txt)")
            print(f"{'='*50}")
            print(f"Next start hex: {next_info.get('next_start_hex')}")
            print(f"Progress: {next_info.get('batches_completed')}/{next_info.get('total_batches')} batches")
            print(f"GPU IDs: {next_info.get('gpu_ids', [0])}")
            print(f"Address: {next_info.get('address')}")
            print(f"Saved at: {next_info.get('timestamp')}")
            print(f"To continue: python3 xiebo.py --continue")
        
        sys.exit(0)
    
    # Continue mode
    if sys.argv[1] == "--continue":
        next_info = load_next_batch_info()
        if not next_info:
            print("❌ No saved state found. Run with --batch first.")
            sys.exit(1)
        
        print(f"\n{'='*60}")
        print(f"CONTINUE MODE (State saved in log)")
        print(f"{'='*60}")
        print(f"Resuming from saved state...")
        
        gpu_ids = next_info['gpu_ids']
        start_hex = next_info['next_start_hex']
        range_bits = int(next_info['original_range_bits'])
        address = next_info['address']
        batches_completed = int(next_info['batches_completed'])
        total_batches = int(next_info['total_batches'])
        
        print(f"Next start: 0x{start_hex}")
        print(f"GPU IDs: {gpu_ids}")
        print(f"Batches completed: {batches_completed}")
        print(f"Total batches: {total_batches}")
        print(f"Address: {address}")
        print(f"Timestamp: {next_info.get('timestamp', 'unknown')}")
        print(f"{'='*60}")
        
        # Hitung jumlah batch yang tersisa
        remaining_batches = total_batches - batches_completed
        batches_to_run = min(remaining_batches, MAX_BATCHES_PER_RUN)
        
        if batches_to_run <= 0:
            print("✅ All batches already completed!")
            sys.exit(0)
        
        print(f"\nRunning {batches_to_run} batches (max {MAX_BATCHES_PER_RUN} per run)")
        print(f"{remaining_batches} batches remaining in total")
        
        # Tanya user mau mode apa
        print(f"\n{'='*60}")
        print("SELECT EXECUTION MODE:")
        print(f"{'='*60}")
        print("1. Sequential (one batch at a time)")
        print("2. Parallel (multiple batches simultaneously)")
        print(f"{'='*60}")
        
        choice = input("Enter choice (1 or 2, default: 2): ").strip()
        
        if choice == "1" or len(gpu_ids) == 1:
            # Sequential mode
            # Inisialisasi log untuk batch yang akan dijalankan
            initialize_batch_log(start_hex, range_bits, address, gpu_ids, batches_to_run, BATCH_SIZE, 
                               start_batch_id=batches_completed, save_state_early=False)
            
            # Jalankan secara sequential
            results = run_xiebo_sequential_mode(gpu_ids, start_hex, range_bits, address, 
                                               batches_to_run, batches_completed)
        else:
            # Parallel mode
            # Inisialisasi log untuk batch yang akan dijalankan
            initialize_batch_log(start_hex, range_bits, address, gpu_ids, batches_to_run, BATCH_SIZE, 
                               start_batch_id=batches_completed, save_state_early=False)
            
            # Jalankan secara parallel
            results = run_xiebo_parallel_mode(gpu_ids, start_hex, range_bits, address, 
                                             batches_to_run, batches_completed)
        
        # Hitung berapa batch yang berhasil diselesaikan
        successful_batches = sum(1 for r in results if r['return_code'] == 0)
        batches_actually_completed = batches_completed + successful_batches
        
        # Update state untuk batch berikutnya
        if batches_actually_completed < total_batches and not STOP_SEARCH_FLAG:
            next_batch_id = batches_actually_completed
            next_start_int = int(start_hex, 16) + (successful_batches * BATCH_SIZE)
            next_start_hex = format(next_start_int, 'x')
            
            # Update state info
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            save_next_batch_info(
                next_info.get('original_start', start_hex),
                range_bits,
                address,
                next_start_hex,
                next_batch_id,
                total_batches,
                gpu_ids,
                timestamp
            )
            
            print(f"\n📝 Updated state for next run.")
            print(f"   Next start: 0x{next_start_hex}")
            print(f"   To continue: python3 xiebo.py --continue")
        
        print(f"\n{'='*60}")
        if STOP_SEARCH_FLAG:
            print(f"🎯 SEARCH STOPPED - PRIVATE KEY FOUND!")
        else:
            if batches_actually_completed >= total_batches:
                print(f"🎉 ALL BATCHES COMPLETED!")
            else:
                print(f"⏸️  BATCHES PAUSED - READY FOR NEXT RUN")
        print(f"{'='*60}")
        
        # Tampilkan summary
        display_compact_summary()
        sys.exit(0)
    
    # Single/Multi-GPU run mode (tanpa --batch atau --parallel)
    if len(sys.argv) == 5:
        gpu_ids_str = sys.argv[1]
        start_hex = sys.argv[2]
        range_bits = int(sys.argv[3])
        address = sys.argv[4]
        
        gpu_ids = parse_gpu_ids(gpu_ids_str)
        
        print(f"\n{'='*60}")
        if len(gpu_ids) == 1:
            print(f"SINGLE GPU RUN MODE")
        else:
            print(f"MULTI-GPU SINGLE BATCH MODE")
        print(f"{'='*60}")
        print(f"GPU IDs: {gpu_ids}")
        print(f"Start: 0x{start_hex}")
        print(f"Range: {range_bits} bits")
        print(f"Address: {address}")
        print(f"{'='*60}")
        
        # Untuk single run, jalankan batch tunggal di GPU pertama
        gpu_id = gpu_ids[0] if gpu_ids else 0
        return_code, found_info = run_xiebo_single_batch(gpu_id, start_hex, range_bits, address)
        
        return return_code
    
    # Parallel batch mode
    elif sys.argv[1] == "--parallel" and len(sys.argv) == 7:
        gpu_ids_str = sys.argv[2]
        start_hex = sys.argv[3]
        range_bits = int(sys.argv[4])
        address = sys.argv[5]
        try:
            num_batches_to_run = int(sys.argv[6])
        except ValueError:
            print(f"❌ Invalid batch count: {sys.argv[6]}")
            sys.exit(1)
        
        gpu_ids = parse_gpu_ids(gpu_ids_str)
        
        # Calculate total
        start_int = int(start_hex, 16)
        total_keys = 1 << range_bits
        end_int = start_int + total_keys - 1
        
        total_batches_needed = math.ceil(total_keys / BATCH_SIZE)
        
        # Limit to MAX_BATCHES_PER_RUN
        num_batches_to_run = min(num_batches_to_run, MAX_BATCHES_PER_RUN, total_batches_needed)
        
        print(f"\n{'='*60}")
        print(f"PARALLEL BATCH MODE")
        print(f"{'='*60}")
        print(f"GPU IDs: {gpu_ids}")
        print(f"GPUs: {len(gpu_ids)}")
        print(f"Start: 0x{start_hex}")
        print(f"Range: {range_bits} bits")
        print(f"Total keys: {total_keys:,}")
        print(f"End: 0x{format(end_int, 'x')}")
        print(f"Batch size: {BATCH_SIZE:,} keys")
        print(f"Address: {address}")
        print(f"Batches to run: {num_batches_to_run}")
        print(f"Log file: {LOG_FILE} (with state info)")
        print(f"Next batch file: {NEXT_BATCH_FILE}")
        print(f"Max batches per run: {MAX_BATCHES_PER_RUN}")
        print(f"Parallel execution: YES (each GPU processes separate batches)")
        print(f"State saved: EARLY (at initialization)")
        print(f"⚠️  AUTO-STOP: Pencarian akan berhenti otomatis jika ditemukan Found: 1 atau lebih")
        print(f"{'='*60}")
        
        # Calculate total batches needed
        total_batches_needed = math.ceil(total_keys / BATCH_SIZE)
        
        print(f"\nTotal batches needed: {total_batches_needed:,}")
        print(f"Batches to run this session: {num_batches_to_run} (max {MAX_BATCHES_PER_RUN})")
        
        if num_batches_to_run < total_batches_needed:
            print(f"Remaining batches for next run: {total_batches_needed - num_batches_to_run:,}")
            print(f"⚠️  State will be saved EARLY before running any batches")
        
        # ⭐ PERUBAHAN UTAMA: Inisialisasi dan simpan state DI AWAL
        initialize_batch_log(start_hex, range_bits, address, gpu_ids, num_batches_to_run, BATCH_SIZE, 
                           start_batch_id=0, save_state_early=True)
        
        # Jalankan batch secara parallel
        results = run_xiebo_parallel_mode(gpu_ids, start_hex, range_bits, address, num_batches_to_run)
        
        # Update state info jika sudah menyelesaikan semua batch yang dijadwalkan
        if num_batches_to_run < total_batches_needed and not STOP_SEARCH_FLAG:
            # State sudah disimpan di awal, tapi kita update progress-nya
            next_start_int = start_int + (num_batches_to_run * BATCH_SIZE)
            next_start_hex = format(next_start_int, 'x')
            
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            save_next_batch_info(
                start_hex,
                range_bits,
                address,
                next_start_hex,
                num_batches_to_run,  # Sekarang sudah selesai
                total_batches_needed,
                gpu_ids,
                timestamp
            )
            
            print(f"\n📝 Updated state with completed progress.")
            print(f"   Next start: 0x{next_start_hex}")
            print(f"   Batches completed: {num_batches_to_run}")
            print(f"   Total batches needed: {total_batches_needed}")
            print(f"   GPU IDs: {gpu_ids}")
            print(f"   To continue: python3 xiebo.py --continue")
        
        print(f"\n{'='*60}")
        if STOP_SEARCH_FLAG:
            print(f"🎯 SEARCH STOPPED - PRIVATE KEY FOUND!")
        else:
            if num_batches_to_run >= total_batches_needed:
                print(f"🎉 ALL BATCHES COMPLETED!")
            else:
                print(f"⏸️  BATCHES PAUSED - READY FOR NEXT RUN")
                print(f"💾 State already saved at initialization")
        print(f"{'='*60}")
        
        # Tampilkan summary ringkas
        display_compact_summary()
        
        # Cek jika ada private key yang ditemukan
        total_batches_log, found_count, _, _ = get_log_summary()
        if found_count and found_count > 0:
            print(f"\n🔥 {found_count} PRIVATE KEY(S) FOUND!")
            print(f"   Check {LOG_FILE} for batch details and WIF keys")
        
    # Sequential batch mode
    elif sys.argv[1] == "--batch" and len(sys.argv) == 6:
        gpu_ids_str = sys.argv[2]
        start_hex = sys.argv[3]
        range_bits = int(sys.argv[4])
        address = sys.argv[5]
        
        gpu_ids = parse_gpu_ids(gpu_ids_str)
        
        # Calculate total
        start_int = int(start_hex, 16)
        total_keys = 1 << range_bits
        end_int = start_int + total_keys - 1
        
        total_batches_needed = math.ceil(total_keys / BATCH_SIZE)
        
        # Limit to MAX_BATCHES_PER_RUN
        batches_to_run = min(total_batches_needed, MAX_BATCHES_PER_RUN)
        
        print(f"\n{'='*60}")
        if len(gpu_ids) == 1:
            print(f"SEQUENTIAL BATCH MODE (SINGLE GPU)")
        else:
            print(f"SEQUENTIAL BATCH MODE (MULTI-GPU, but running sequentially)")
        print(f"{'='*60}")
        print(f"GPU IDs: {gpu_ids}")
        print(f"Start: 0x{start_hex}")
        print(f"Range: {range_bits} bits")
        print(f"Total keys: {total_keys:,}")
        print(f"End: 0x{format(end_int, 'x')}")
        print(f"Batch size: {BATCH_SIZE:,} keys")
        print(f"Address: {address}")
        print(f"Log file: {LOG_FILE} (with state info)")
        print(f"Next batch file: {NEXT_BATCH_FILE}")
        print(f"Max batches per run: {MAX_BATCHES_PER_RUN}")
        print(f"Parallel execution: NO (batches run one by one)")
        print(f"State saved: EARLY (at initialization)")
        print(f"⚠️  AUTO-STOP: Pencarian akan berhenti otomatis jika ditemukan Found: 1 atau lebih")
        print(f"{'='*60}")
        
        print(f"\nTotal batches needed: {total_batches_needed:,}")
        print(f"Batches to run this session: {batches_to_run} (max {MAX_BATCHES_PER_RUN})")
        
        if batches_to_run < total_batches_needed:
            print(f"Remaining batches for next run: {total_batches_needed - batches_to_run:,}")
            print(f"⚠️  State will be saved EARLY before running any batches")
        
        # ⭐ PERUBAHAN UTAMA: Inisialisasi dan simpan state DI AWAL
        initialize_batch_log(start_hex, range_bits, address, gpu_ids, batches_to_run, BATCH_SIZE, 
                           start_batch_id=0, save_state_early=True)
        
        # Jalankan batch secara sequential
        results = run_xiebo_sequential_mode(gpu_ids, start_hex, range_bits, address, batches_to_run)
        
        # Update state info jika sudah menyelesaikan semua batch yang dijadwalkan
        if batches_to_run < total_batches_needed and not STOP_SEARCH_FLAG:
            # State sudah disimpan di awal, tapi kita update progress-nya
            next_start_int = start_int + (batches_to_run * BATCH_SIZE)
            next_start_hex = format(next_start_int, 'x')
            
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            save_next_batch_info(
                start_hex,
                range_bits,
                address,
                next_start_hex,
                batches_to_run,  # Sekarang sudah selesai
                total_batches_needed,
                gpu_ids,
                timestamp
            )
            
            print(f"\n📝 Updated state with completed progress.")
            print(f"   Next start: 0x{next_start_hex}")
            print(f"   Batches completed: {batches_to_run}")
            print(f"   Total batches needed: {total_batches_needed}")
            print(f"   GPU IDs: {gpu_ids}")
            print(f"   To continue: python3 xiebo.py --continue")
        
        print(f"\n{'='*60}")
        if STOP_SEARCH_FLAG:
            print(f"🎯 SEARCH STOPPED - PRIVATE KEY FOUND!")
        else:
            if batches_to_run >= total_batches_needed:
                print(f"🎉 ALL BATCHES COMPLETED!")
            else:
                print(f"⏸️  BATCHES PAUSED - READY FOR NEXT RUN")
                print(f"💾 State already saved at initialization")
        print(f"{'='*60}")
        
        # Tampilkan summary ringkas
        display_compact_summary()
        
        # Cek jika ada private key yang ditemukan
        total_batches_log, found_count, _, _ = get_log_summary()
        if found_count and found_count > 0:
            print(f"\n🔥 {found_count} PRIVATE KEY(S) FOUND!")
            print(f"   Check {LOG_FILE} for batch details and WIF keys")
        
    else:
        print("Invalid arguments")
        print("Usage: python3 xiebo.py GPU_IDS START_HEX RANGE_BITS ADDRESS")
        print("       (GPU_IDS can be single: '0' or multiple: '0 1')")
        print("Or:    python3 xiebo.py --batch GPU_IDS START_HEX RANGE_BITS ADDRESS (sequential)")
        print("Or:    python3 xiebo.py --parallel GPU_IDS START_HEX RANGE_BITS ADDRESS BATCH_COUNT")
        print("Or:    python3 xiebo.py --summary")
        print("Or:    python3 xiebo.py --continue")
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
