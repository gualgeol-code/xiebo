import subprocess
import sys
import os
import time
import math
import re
import pyodbc
import threading
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, as_completed

# Konfigurasi database SQL Server
SERVER = "benilapo-31088.portmap.host,31088"
DATABASE = "puckok"
USERNAME = "sa"
PASSWORD = "LEtoy_89"
TABLE = "dbo.Tbatch"

# Global flag untuk menghentikan pencarian
STOP_SEARCH_FLAG = False

# Konfigurasi batch
MAX_BATCHES_PER_RUN = 4000000000000  # Maksimal 1juta batch per eksekusi

def connect_db():
    """Membuat koneksi ke database SQL Server"""
    try:
        conn = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={SERVER};"
            f"DATABASE={DATABASE};"
            f"UID={USERNAME};"
            f"PWD={PASSWORD};"
            "Encrypt=no;"
            "TrustServerCertificate=yes;"
            "Connection Timeout=30;",
            autocommit=False
        )
        return conn
    except Exception as e:
        print(f"❌ Database connection error: {e}")
        return None

def get_batch_by_id(batch_id):
    """Mengambil data batch berdasarkan ID"""
    conn = connect_db()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        
        # Ambil data batch berdasarkan ID
        cursor.execute(f"""
            SELECT id, start_range, end_range, status, found, wif
            FROM {TABLE} 
            WHERE id = ?
        """, (batch_id,))
        
        row = cursor.fetchone()
        
        if row:
            columns = [column[0] for column in cursor.description]
            batch = dict(zip(columns, row))
        else:
            batch = None
        
        cursor.close()
        conn.close()
        
        return batch
        
    except Exception as e:
        print(f"❌ Error getting batch by ID: {e}")
        if conn:
            conn.close()
        return None

def get_batch_range(batch_ids):
    """Mengambil data batch untuk multiple ID"""
    conn = connect_db()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        
        # Buat placeholder untuk parameter
        placeholders = ','.join('?' for _ in batch_ids)
        
        # Ambil data batch untuk multiple ID
        cursor.execute(f"""
            SELECT id, start_range, end_range, status, found, wif
            FROM {TABLE} 
            WHERE id IN ({placeholders})
            ORDER BY id
        """, batch_ids)
        
        batches = []
        rows = cursor.fetchall()
        
        for row in rows:
            columns = [column[0] for column in cursor.description]
            batch = dict(zip(columns, row))
            batches.append(batch)
        
        cursor.close()
        conn.close()
        
        return batches
        
    except Exception as e:
        print(f"❌ Error getting batch range: {e}")
        if conn:
            conn.close()
        return []

def get_available_batches_in_range(start_id, end_id, count, exclude_ids=[]):
    """Mengambil beberapa batch yang tersedia dalam range tertentu"""
    conn = connect_db()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        
        # Buat query untuk mengambil batch yang tersedia dalam range
        exclude_condition = ""
        if exclude_ids:
            exclude_ids_str = ",".join(str(id) for id in exclude_ids)
            exclude_condition = f"AND id NOT IN ({exclude_ids_str})"
        
        cursor.execute(f"""
            SELECT TOP {count} id, start_range, end_range, status
            FROM {TABLE} 
            WHERE id >= ? AND id <= ?
            AND status NOT IN ('done', 'inprogress')
            {exclude_condition}
            ORDER BY id
        """, (start_id, end_id))
        
        batches = []
        rows = cursor.fetchall()
        
        for row in rows:
            columns = [column[0] for column in cursor.description]
            batch = dict(zip(columns, row))
            batches.append(batch)
        
        cursor.close()
        conn.close()
        
        return batches
        
    except Exception as e:
        print(f"❌ Error getting available batches in range: {e}")
        if conn:
            conn.close()
        return []

def update_batch_status(batch_id, status, found='', wif=''):
    """Update status batch di database"""
    conn = connect_db()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # Update status batch
        cursor.execute(f"""
            UPDATE {TABLE} 
            SET status = ?, found = ?, wif = ?
            WHERE id = ?
        """, (status, found, wif, batch_id))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"📝 Updated batch {batch_id}: status={status}, found={found}")
        return True
        
    except Exception as e:
        print(f"❌ Error updating batch status: {e}")
        if conn:
            conn.rollback()
            conn.close()
        return False

def calculate_range_bits(start_hex, end_hex):
    """Menghitung range bits dari start dan end hex"""
    try:
        start_int = int(start_hex, 16)
        end_int = int(end_hex, 16)
        
        # Hitung jumlah keys
        keys_count = end_int - start_int + 1
        
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
            
    except Exception as e:
        print(f"❌ Error calculating range bits: {e}")
        return 64  # Default value

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

def display_xiebo_output_real_time(process, gpu_id, batch_id=None):
    """Menampilkan output xiebo secara real-time"""
    gpu_prefix = f"[GPU{gpu_id}]"
    batch_prefix = f"[Batch{batch_id}]" if batch_id else ""
    prefix = f"{gpu_prefix}{batch_prefix}"
    
    print(f"\n{prefix} {'─' * (80 - len(prefix))}")
    print(f"{prefix} 🎯 XIEBO OUTPUT (REAL-TIME):")
    print(f"{prefix} {'─' * (80 - len(prefix))}")
    
    output_lines = []
    while True:
        output_line = process.stdout.readline()
        if output_line == '' and process.poll() is not None:
            break
        if output_line:
            # Tampilkan output dengan format yang lebih baik
            stripped_line = output_line.strip()
            if stripped_line:
                # Warna untuk output tertentu
                line_lower = stripped_line.lower()
                if 'found:' in line_lower or 'success' in line_lower:
                    # Line dengan hasil ditemukan (warna hijau)
                    print(f"{prefix} \033[92m{stripped_line}\033[0m")
                elif 'error' in line_lower or 'failed' in line_lower:
                    # Line dengan error (warna merah)
                    print(f"{prefix} \033[91m{stripped_line}\033[0m")
                elif 'speed' in line_lower or 'key/s' in line_lower:
                    # Line dengan informasi speed (warna kuning)
                    print(f"{prefix} \033[93m{stripped_line}\033[0m")
                elif 'range' in line_lower:
                    # Line dengan informasi range (warna biru)
                    print(f"{prefix} \033[94m{stripped_line}\033[0m")
                else:
                    # Line normal (warna default)
                    print(f"{prefix} {stripped_line}")
            output_lines.append(output_line)
    
    output_text = ''.join(output_lines)
    print(f"{prefix} {'─' * (80 - len(prefix))}")
    
    return output_text

def run_xiebo_on_gpu(gpu_id, start_hex, range_bits, address, batch_id=None):
    """Run xiebo binary langsung pada GPU tertentu"""
    global STOP_SEARCH_FLAG
    
    cmd = ["./xiebo", "-gpuId", str(gpu_id), "-start", start_hex, 
           "-range", str(range_bits), address]
    
    gpu_prefix = f"[GPU{gpu_id}]"
    batch_prefix = f"[Batch{batch_id}]" if batch_id else ""
    prefix = f"{gpu_prefix}{batch_prefix}"
    
    print(f"\n{prefix} {'=' * (80 - len(prefix))}")
    print(f"{prefix} 🚀 STARTING XIEBO EXECUTION")
    print(f"{prefix} {'=' * (80 - len(prefix))}")
    print(f"{prefix} Command: {' '.join(cmd)}")
    print(f"{prefix} Batch ID: {batch_id if batch_id is not None else 'N/A'}")
    print(f"{prefix} {'=' * (80 - len(prefix))}")
    
    try:
        # Update status menjadi inprogress jika ada batch_id
        if batch_id is not None:
            update_batch_status(batch_id, 'inprogress')
        
        # Jalankan xiebo dan tampilkan output secara real-time
        print(f"{prefix} ⏳ Launching xiebo process...")
        
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
        output_text = display_xiebo_output_real_time(process, gpu_id, batch_id)
        
        # Tunggu proses selesai
        return_code = process.wait()
        
        # Parse output untuk mencari private key
        found_info = parse_xiebo_output(output_text)
        
        # Update status berdasarkan hasil
        if batch_id is not None:
            # Tentukan nilai 'found' berdasarkan found_count atau found status
            if found_info['found_count'] > 0:
                found_status = 'Yes'
            elif found_info['found']:
                found_status = 'Yes'
            else:
                found_status = 'No'
            
            # Simpan WIF key jika ditemukan
            wif_key = found_info['wif_key'] if found_info['wif_key'] else ''
            
            # Update status di database
            update_batch_status(batch_id, 'done', found_status, wif_key)
        
        # Tampilkan ringkasan hasil pencarian
        print(f"\n{prefix} {'=' * (80 - len(prefix))}")
        print(f"{prefix} 📊 SEARCH RESULT SUMMARY")
        print(f"{prefix} {'=' * (80 - len(prefix))}")
        
        if found_info['found_count'] > 0:
            print(f"{prefix} \033[92m✅ FOUND: {found_info['found_count']} PRIVATE KEY(S)!\033[0m")
        elif found_info['found']:
            print(f"{prefix} \033[92m✅ PRIVATE KEY FOUND!\033[0m")
        else:
            print(f"{prefix} \033[93m❌ Private key not found in this batch\033[0m")
        
        if found_info['speed_info']:
            print(f"{prefix} 📈 Performance: {found_info['speed_info']}")
        
        if found_info['found'] or found_info['found_count'] > 0:
            print(f"{prefix} 📋 Found information:")
            if found_info['raw_output']:
                for line in found_info['raw_output'].split('\n'):
                    if 'found:' in line.lower() or 'priv' in line.lower():
                        print(f"{prefix} \033[92m{line}\033[0m")
                    else:
                        print(f"{prefix} {line}")
        
        print(f"{prefix} {'=' * (80 - len(prefix))}")
        
        # Tampilkan return code
        if return_code == 0:
            print(f"{prefix} 🟢 Process completed successfully (return code: {return_code})")
        else:
            print(f"{prefix} 🟡 Process completed with return code: {return_code}")
        
        return return_code, found_info
        
    except KeyboardInterrupt:
        print(f"\n\n{prefix} {'=' * (80 - len(prefix))}")
        print(f"{prefix} ⚠️  STOPPED BY USER INTERRUPT (Ctrl+C)")
        print(f"{prefix} {'=' * (80 - len(prefix))}")
        
        # Update status jika batch diinterupsi
        if batch_id is not None:
            update_batch_status(batch_id, 'interrupted')
        
        return 130, {'found': False}
    except Exception as e:
        error_msg = str(e)
        print(f"\n{prefix} {'=' * (80 - len(prefix))}")
        print(f"{prefix} ❌ ERROR OCCURRED")
        print(f"{prefix} {'=' * (80 - len(prefix))}")
        print(f"{prefix} Error: {error_msg}")
        print(f"{prefix} {'=' * (80 - len(prefix))}")
        
        # Update status error jika ada batch_id
        if batch_id is not None:
            update_batch_status(batch_id, 'error')
        
        return 1, {'found': False}

def gpu_worker(gpu_id, batch_ids, address, results_queue):
    """Worker thread untuk memproses multiple batch pada satu GPU"""
    global STOP_SEARCH_FLAG
    
    worker_id = f"GPU{gpu_id}"
    total_batches = len(batch_ids)
    processed_count = 0
    
    print(f"\n🎬 {worker_id}: Worker thread started for {total_batches} batch(es)")
    
    for i, batch_id in enumerate(batch_ids):
        if STOP_SEARCH_FLAG:
            print(f"{worker_id}: ⏹️  Stopping early due to STOP_SEARCH_FLAG")
            break
        
        print(f"\n{worker_id}: Processing batch {i+1}/{total_batches} (ID: {batch_id})")
        
        # Ambil data batch
        batch = get_batch_by_id(batch_id)
        
        if not batch:
            print(f"{worker_id}: ❌ Batch ID {batch_id} not found. Skipping.")
            results_queue.put({
                'gpu_id': gpu_id,
                'batch_id': batch_id,
                'status': 'not_found',
                'found': False
            })
            continue
        
        # Cek status batch
        status = batch.get('status', '').strip()
        
        if status == 'done':
            print(f"{worker_id}: ⏭️  Batch ID {batch_id} already done. Skipping.")
            results_queue.put({
                'gpu_id': gpu_id,
                'batch_id': batch_id,
                'status': 'already_done',
                'found': False
            })
            continue
        
        if status == 'inprogress':
            print(f"{worker_id}: ⏭️  Batch ID {batch_id} is in progress. Skipping.")
            results_queue.put({
                'gpu_id': gpu_id,
                'batch_id': batch_id,
                'status': 'in_progress',
                'found': False
            })
            continue
        
        # Ambil data range
        start_range = batch['start_range']
        end_range = batch['end_range']
        
        # Hitung range bits
        range_bits = calculate_range_bits(start_range, end_range)
        
        # Run batch pada GPU ini
        print(f"{worker_id}: ▶️  BATCH {i+1}/{total_batches} (ID: {batch_id})")
        print(f"{worker_id}: Start Range: {start_range}")
        print(f"{worker_id}: End Range: {end_range}")
        print(f"{worker_id}: Range Bits: {range_bits}")
        print(f"{worker_id}: Address: {address}")
        
        return_code, found_info = run_xiebo_on_gpu(gpu_id, start_range, range_bits, address, batch_id)
        
        # Kirim hasil ke queue
        results_queue.put({
            'gpu_id': gpu_id,
            'batch_id': batch_id,
            'return_code': return_code,
            'found': found_info.get('found', False),
            'found_count': found_info.get('found_count', 0),
            'status': 'completed'
        })
        
        processed_count += 1
        
        # Delay antara batch (kecuali jika STOP_SEARCH_FLAG aktif atau batch terakhir)
        if not STOP_SEARCH_FLAG and i < total_batches - 1:
            time.sleep(2)
    
    print(f"\n🏁 {worker_id}: Worker thread finished. Processed {processed_count}/{total_batches} batches.")

def distribute_batches_to_gpus(gpu_ids, batch_ids):
    """Distribusikan batch IDs ke masing-masing GPU"""
    total_batches = len(batch_ids)
    total_gpus = len(gpu_ids)
    
    # Hitung jumlah batch per GPU
    base_batches_per_gpu = total_batches // total_gpus
    remainder = total_batches % total_gpus
    
    distribution = {}
    start_index = 0
    
    for i, gpu_id in enumerate(gpu_ids):
        # GPU dengan indeks awal dapat batch ekstra jika ada remainder
        batches_for_this_gpu = base_batches_per_gpu + (1 if i < remainder else 0)
        end_index = start_index + batches_for_this_gpu
        
        distribution[gpu_id] = batch_ids[start_index:end_index]
        start_index = end_index
    
    return distribution

def process_batch_range_single_gpu(gpu_id, start_id, end_id, address):
    """Process batch range pada single GPU"""
    global STOP_SEARCH_FLAG
    
    print(f"\n{'='*80}")
    print(f"🚀 SINGLE GPU BATCH RANGE MODE")
    print(f"{'='*80}")
    print(f"GPU: {gpu_id}")
    print(f"Batch Range: {start_id} to {end_id}")
    print(f"Total Batches: {end_id - start_id + 1}")
    print(f"Address: {address}")
    print(f"{'='*80}")
    
    current_id = start_id
    batches_processed = 0
    total_to_process = end_id - start_id + 1
    
    # Generate semua batch IDs dalam range
    batch_ids = list(range(start_id, end_id + 1))
    
    # Ambil informasi batch sekaligus
    batches = get_batch_range(batch_ids)
    batch_map = {batch['id']: batch for batch in batches}
    
    for batch_id in batch_ids:
        if STOP_SEARCH_FLAG:
            print(f"\n⏹️  Stopping early due to STOP_SEARCH_FLAG")
            break
        
        batches_processed += 1
        print(f"\n📋 Processing batch {batches_processed}/{total_to_process} (ID: {batch_id})")
        
        # Ambil data batch dari map
        batch = batch_map.get(batch_id)
        
        if not batch:
            print(f"❌ Batch ID {batch_id} not found in database. Skipping.")
            continue
        
        # Cek status batch
        status = batch.get('status', '').strip()
        
        if status == 'done':
            print(f"⏭️  Batch ID {batch_id} already done. Skipping.")
            continue
        
        if status == 'inprogress':
            print(f"⏭️  Batch ID {batch_id} is in progress. Skipping.")
            continue
        
        # Ambil data range
        start_range = batch['start_range']
        end_range = batch['end_range']
        
        # Hitung range bits
        range_bits = calculate_range_bits(start_range, end_range)
        
        # Run batch
        print(f"\n▶️  BATCH {batches_processed}/{total_to_process} (ID: {batch_id})")
        print(f"Start Range: {start_range}")
        print(f"End Range: {end_range}")
        print(f"Range Bits: {range_bits}")
        
        return_code, found_info = run_xiebo_on_gpu(gpu_id, start_range, range_bits, address, batch_id)
        
        # Tampilkan progress
        if batches_processed % 5 == 0 or STOP_SEARCH_FLAG:
            print(f"\n📈 Progress: {batches_processed}/{total_to_process} batches processed")
        
        # Delay antara batch
        if not STOP_SEARCH_FLAG and batches_processed < total_to_process:
            time.sleep(2)
    
    print(f"\n{'='*80}")
    if STOP_SEARCH_FLAG:
        print(f"🎯 SEARCH STOPPED - PRIVATE KEY FOUND!")
    else:
        print(f"✅ BATCH RANGE PROCESSING COMPLETED")
    print(f"{'='*80}")
    
    print(f"\n📋 Single GPU Range Summary:")
    print(f"  GPU ID: {gpu_id}")
    print(f"  Batch Range: {start_id} to {end_id}")
    print(f"  Batches processed: {batches_processed}/{total_to_process}")
    
    if STOP_SEARCH_FLAG:
        print(f"\n🔥 PRIVATE KEY FOUND!")
        print(f"   Check database table Tbatch for details")

def main():
    global STOP_SEARCH_FLAG
    
    # Reset flag stop search setiap kali program dijalankan
    STOP_SEARCH_FLAG = False
    
    # Parse arguments
    if len(sys.argv) < 2:
        print("Xiebo Batch Runner with SQL Server Database - MULTI GPU & MULTI BATCH SUPPORT")
        print("Usage:")
        print("  Single run: python3 bm.py GPU_ID START_HEX RANGE_BITS ADDRESS")
        print("  Batch run single GPU: python3 bm.py --batch-db GPU_ID START_ID ADDRESS")
        print("  Batch range single GPU: python3 bm.py --batch-range GPU_ID START_ID END_ID ADDRESS")
        print("  Multi GPU with range: python3 bm.py --multi-gpu-range GPU_IDS START_ID END_ID ADDRESS")
        print("  Multi GPU with specific batches: python3 bm.py --multi-batch GPU_IDS BATCH_IDS ADDRESS")
        print("\nExamples:")
        print("  Single GPU single batch: python3 bm.py --batch-db 0 1 1ABC...")
        print("  Single GPU batch range: python3 bm.py --batch-range 0 1 100 1ABC...")
        print("  Multi GPU batch range: python3 bm.py --multi-gpu-range 0,1,2 1 100 1ABC...")
        print("  Multi GPU specific batches: python3 bm.py --multi-batch 0,1,2 1,5,10,15 1ABC...")
        print("\n⚠️  FEATURES:")
        print("  - Menggunakan database SQL Server")
        print("  - Multi-GPU support")
        print("  - Multi-Batch ID support (range atau list spesifik)")
        print(f"  - Maksimal {MAX_BATCHES_PER_RUN} batch per eksekusi")
        print("  - Auto-stop ketika ditemukan Found: 1 atau lebih")
        sys.exit(1)
    
    # Multi-GPU dengan specific batch IDs
    if sys.argv[1] == "--multi-batch" and len(sys.argv) == 6:
        gpu_ids_str = sys.argv[2]
        batch_ids_str = sys.argv[3]
        address = sys.argv[4]
        
        # Parse GPU IDs
        gpu_ids = [int(gpu_id.strip()) for gpu_id in gpu_ids_str.split(',') if gpu_id.strip().isdigit()]
        
        # Parse Batch IDs (support range: 1-100 atau list: 1,5,10,15)
        batch_ids = []
        if '-' in batch_ids_str:
            # Format range: 1-100
            try:
                start_id, end_id = map(int, batch_ids_str.split('-'))
                batch_ids = list(range(start_id, end_id + 1))
            except ValueError:
                print(f"❌ Error: Invalid batch range format. Use 'START-END' (e.g., '1-100')")
                sys.exit(1)
        else:
            # Format list: 1,5,10,15
            batch_ids = [int(bid.strip()) for bid in batch_ids_str.split(',') if bid.strip().isdigit()]
        
        if not gpu_ids:
            print("❌ Error: Invalid GPU IDs format. Use comma-separated values (e.g., 0,1,2)")
            sys.exit(1)
        
        if not batch_ids:
            print("❌ Error: Invalid Batch IDs format. Use comma-separated list or range (e.g., '1,5,10' or '1-100')")
            sys.exit(1)
        
        print(f"\n{'='*80}")
        print(f"🚀 MULTI-GPU SPECIFIC BATCHES MODE")
        print(f"{'='*80}")
        print(f"GPU IDs: {gpu_ids}")
        print(f"Batch IDs: {batch_ids[:10]}{'...' if len(batch_ids) > 10 else ''}")
        print(f"Total Batches: {len(batch_ids)}")
        print(f"Address: {address}")
        print(f"{'='*80}")
        
        # Distribusikan batch ke GPU
        distribution = distribute_batches_to_gpus(gpu_ids, batch_ids)
        
        # Tampilkan distribusi
        print(f"\n📊 Batch Distribution:")
        for gpu_id, gpu_batches in distribution.items():
            print(f"  GPU {gpu_id}: {len(gpu_batches)} batch(es)")
            if gpu_batches:
                print(f"    IDs: {gpu_batches[:5]}{'...' if len(gpu_batches) > 5 else ''}")
        
        # Setup untuk multi-threading
        results_queue = Queue()
        threads = []
        
        # Jalankan worker thread untuk setiap GPU dengan batch-nya
        for gpu_id, gpu_batches in distribution.items():
            if gpu_batches:  # Hanya buat thread jika ada batch untuk GPU ini
                thread = threading.Thread(
                    target=gpu_worker,
                    args=(gpu_id, gpu_batches, address, results_queue),
                    daemon=True
                )
                threads.append(thread)
                thread.start()
                print(f"✅ Started worker thread for GPU {gpu_id} with {len(gpu_batches)} batches")
                time.sleep(1)  # Stagger thread startup
        
        # Tunggu semua thread selesai
        try:
            for thread in threads:
                thread.join()
        except KeyboardInterrupt:
            print(f"\n\n{'='*80}")
            print(f"⚠️  STOPPED BY USER INTERRUPT (Ctrl+C)")
            print(f"{'='*80}")
            STOP_SEARCH_FLAG = True
            # Tunggu thread untuk selesai dengan graceful shutdown
            for thread in threads:
                thread.join(timeout=5)
        
        # Kumpulkan hasil
        total_batches = 0
        found_batches = 0
        
        while not results_queue.empty():
            result = results_queue.get()
            total_batches += 1
            if result.get('found', False) or result.get('found_count', 0) > 0:
                found_batches += 1
        
        print(f"\n{'='*80}")
        if STOP_SEARCH_FLAG:
            print(f"🎯 SEARCH STOPPED - PRIVATE KEY FOUND!")
        else:
            print(f"✅ MULTI-GPU BATCH PROCESSING COMPLETED")
        print(f"{'='*80}")
        
        print(f"\n📋 Multi-GPU Batch Summary:")
        print(f"  GPUs used: {len(gpu_ids)}")
        print(f"  Total batches assigned: {len(batch_ids)}")
        print(f"  Batches processed: {total_batches}")
        print(f"  Batches with findings: {found_batches}")
        
        if found_batches > 0:
            print(f"\n🔥 PRIVATE KEY(S) FOUND!")
            print(f"   Check database table Tbatch for details")
        
        return 0
    
    # Multi-GPU dengan batch range
    elif sys.argv[1] == "--multi-gpu-range" and len(sys.argv) == 6:
        gpu_ids_str = sys.argv[2]
        start_id = int(sys.argv[3])
        end_id = int(sys.argv[4])
        address = sys.argv[5]
        
        # Parse GPU IDs
        gpu_ids = [int(gpu_id.strip()) for gpu_id in gpu_ids_str.split(',') if gpu_id.strip().isdigit()]
        
        if not gpu_ids:
            print("❌ Error: Invalid GPU IDs format. Use comma-separated values (e.g., 0,1,2)")
            sys.exit(1)
        
        if end_id < start_id:
            print("❌ Error: END_ID must be greater than or equal to START_ID")
            sys.exit(1)
        
        # Generate batch IDs dalam range
        batch_ids = list(range(start_id, end_id + 1))
        
        print(f"\n{'='*80}")
        print(f"🚀 MULTI-GPU BATCH RANGE MODE")
        print(f"{'='*80}")
        print(f"GPU IDs: {gpu_ids}")
        print(f"Batch Range: {start_id} to {end_id}")
        print(f"Total Batches: {len(batch_ids)}")
        print(f"Address: {address}")
        print(f"{'='*80}")
        
        # Distribusikan batch ke GPU
        distribution = distribute_batches_to_gpus(gpu_ids, batch_ids)
        
        # Tampilkan distribusi
        print(f"\n📊 Batch Distribution:")
        for gpu_id, gpu_batches in distribution.items():
            print(f"  GPU {gpu_id}: {len(gpu_batches)} batch(es)")
        
        # Setup untuk multi-threading
        results_queue = Queue()
        threads = []
        
        # Jalankan worker thread untuk setiap GPU dengan batch-nya
        for gpu_id, gpu_batches in distribution.items():
            if gpu_batches:  # Hanya buat thread jika ada batch untuk GPU ini
                thread = threading.Thread(
                    target=gpu_worker,
                    args=(gpu_id, gpu_batches, address, results_queue),
                    daemon=True
                )
                threads.append(thread)
                thread.start()
                print(f"✅ Started worker thread for GPU {gpu_id} with {len(gpu_batches)} batches")
                time.sleep(1)  # Stagger thread startup
        
        # Tunggu semua thread selesai
        try:
            for thread in threads:
                thread.join()
        except KeyboardInterrupt:
            print(f"\n\n{'='*80}")
            print(f"⚠️  STOPPED BY USER INTERRUPT (Ctrl+C)")
            print(f"{'='*80}")
            STOP_SEARCH_FLAG = True
            # Tunggu thread untuk selesai dengan graceful shutdown
            for thread in threads:
                thread.join(timeout=5)
        
        # Kumpulkan hasil
        total_batches = 0
        found_batches = 0
        
        while not results_queue.empty():
            result = results_queue.get()
            total_batches += 1
            if result.get('found', False) or result.get('found_count', 0) > 0:
                found_batches += 1
        
        print(f"\n{'='*80}")
        if STOP_SEARCH_FLAG:
            print(f"🎯 SEARCH STOPPED - PRIVATE KEY FOUND!")
        else:
            print(f"✅ MULTI-GPU RANGE PROCESSING COMPLETED")
        print(f"{'='*80}")
        
        print(f"\n📋 Multi-GPU Range Summary:")
        print(f"  GPUs used: {len(gpu_ids)}")
        print(f"  Batch Range: {start_id} to {end_id}")
        print(f"  Total batches: {len(batch_ids)}")
        print(f"  Batches processed: {total_batches}")
        print(f"  Batches with findings: {found_batches}")
        
        if found_batches > 0:
            print(f"\n🔥 PRIVATE KEY(S) FOUND!")
            print(f"   Check database table Tbatch for details")
        
        return 0
    
    # Single GPU dengan batch range
    elif sys.argv[1] == "--batch-range" and len(sys.argv) == 6:
        gpu_id = sys.argv[2]
        start_id = int(sys.argv[3])
        end_id = int(sys.argv[4])
        address = sys.argv[5]
        
        if end_id < start_id:
            print("❌ Error: END_ID must be greater than or equal to START_ID")
            sys.exit(1)
        
        # Process batch range pada single GPU
        process_batch_range_single_gpu(gpu_id, start_id, end_id, address)
        return 0
    
    # Batch run from database mode (single GPU, single batch start)
    elif sys.argv[1] == "--batch-db" and len(sys.argv) == 5:
        gpu_id = sys.argv[2]
        start_id = int(sys.argv[3])
        address = sys.argv[4]
        
        # Gunakan fungsi batch range dengan end_id = start_id (hanya 1 batch)
        process_batch_range_single_gpu(gpu_id, start_id, start_id, address)
        return 0
    
    # Single run mode (tetap support untuk backward compatibility)
    elif len(sys.argv) == 5:
        gpu_id = sys.argv[1]
        start_hex = sys.argv[2]
        range_bits = int(sys.argv[3])
        address = sys.argv[4]
        
        print(f"\n{'='*80}")
        print(f"🚀 SINGLE RUN MODE")
        print(f"{'='*80}")
        print(f"GPU: {gpu_id}")
        print(f"Start: 0x{start_hex}")
        print(f"Range: {range_bits} bits")
        print(f"Address: {address}")
        print(f"{'='*80}")
        
        return_code, found_info = run_xiebo_on_gpu(gpu_id, start_hex, range_bits, address)
        
        return return_code
    
    else:
        print("Invalid arguments")
        print("Usage: python3 bm.py GPU_ID START_HEX RANGE_BITS ADDRESS")
        print("Or:    python3 bm.py --batch-db GPU_ID START_ID ADDRESS")
        print("Or:    python3 bm.py --batch-range GPU_ID START_ID END_ID ADDRESS")
        print("Or:    python3 bm.py --multi-gpu-range GPU_IDS START_ID END_ID ADDRESS")
        print("Or:    python3 bm.py --multi-batch GPU_IDS BATCH_IDS ADDRESS")
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
    
    # Check for color support
    if os.name == 'posix':
        os.system('')  # Enable ANSI colors on Unix-like systems
    
    main()
