import subprocess
import sys
import os
import time
import math
import re
import pyodbc
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# Konfigurasi database SQL Server
SERVER = "benilapo-31088.portmap.host,31088"
DATABASE = "puckok"
USERNAME = "sa"
PASSWORD = "LEtoy_89"
TABLE = "dbo.Tbatch"

# Global flag untuk menghentikan pencarian
STOP_SEARCH_FLAG = True

# Konfigurasi batch
MAX_BATCHES_PER_RUN = 10  # Maksimal 1juta batch per eksekusi
BATCH_SIZE = 4000000000000  # 2 triliun keys per batch

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

def get_pending_batches(start_id, limit=100):
    """Mengambil batch yang pending mulai dari ID tertentu"""
    conn = connect_db()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        
        # Ambil batch dengan status bukan 'done' atau 'inprogress'
        cursor.execute(f"""
            SELECT id, start_range, end_range, status, found, wif
            FROM {TABLE} 
            WHERE id >= ? AND status NOT IN ('done', 'inprogress')
            ORDER BY id
            OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY
        """, (start_id, limit))
        
        rows = cursor.fetchall()
        batches = []
        
        if rows:
            columns = [column[0] for column in cursor.description]
            for row in rows:
                batches.append(dict(zip(columns, row)))
        
        cursor.close()
        conn.close()
        
        return batches
        
    except Exception as e:
        print(f"❌ Error getting pending batches: {e}")
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

def calculate_range_bits_from_count(keys_count):
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

def display_xiebo_output_real_time(process, gpu_id=None):
    """Menampilkan output xiebo secara real-time"""
    prefix = f"GPU {gpu_id}: " if gpu_id is not None else ""
    
    print(f"\n{'─' * 80}")
    print(f"🎯 XIEBO OUTPUT (REAL-TIME){f' - GPU {gpu_id}' if gpu_id is not None else ''}:")
    print(f"{'─' * 80}")
    
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
                    print(f"\033[92m   {prefix}{stripped_line}\033[0m")
                elif 'error' in line_lower or 'failed' in line_lower:
                    # Line dengan error (warna merah)
                    print(f"\033[91m   {prefix}{stripped_line}\033[0m")
                elif 'speed' in line_lower or 'key/s' in line_lower:
                    # Line dengan informasi speed (warna kuning)
                    print(f"\033[93m   {prefix}{stripped_line}\033[0m")
                elif 'range' in line_lower:
                    # Line dengan informasi range (warna biru)
                    print(f"\033[94m   {prefix}{stripped_line}\033[0m")
                else:
                    # Line normal (warna default)
                    print(f"   {prefix}{stripped_line}")
            output_lines.append(output_line)
    
    output_text = ''.join(output_lines)
    print(f"{'─' * 80}")
    
    return output_text

def run_xiebo(gpu_id, start_hex, range_bits, address, batch_id=None):
    """Run xiebo binary untuk single GPU dengan batch tertentu"""
    global STOP_SEARCH_FLAG
    
    cmd = ["./xiebo", "-gpuId", str(gpu_id), "-start", start_hex, 
           "-range", str(range_bits), address]
    
    print(f"\n{'='*80}")
    print(f"🚀 STARTING XIEBO EXECUTION - GPU {gpu_id}")
    print(f"{'='*80}")
    print(f"Command: {' '.join(cmd)}")
    print(f"Batch ID: {batch_id if batch_id is not None else 'N/A'}")
    print(f"{'='*80}")
    
    try:
        # Update status menjadi inprogress jika ada batch_id
        if batch_id is not None:
            update_batch_status(batch_id, 'inprogress')
        
        # Jalankan xiebo dan tampilkan output secara real-time
        print(f"\n⏳ Launching xiebo process for GPU {gpu_id}...")
        
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
        output_text = display_xiebo_output_real_time(process, gpu_id)
        
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
        print(f"\n{'='*80}")
        print(f"📊 SEARCH RESULT SUMMARY - GPU {gpu_id}")
        print(f"{'='*80}")
        
        if found_info['found_count'] > 0:
            print(f"\033[92m✅ FOUND: {found_info['found_count']} PRIVATE KEY(S)!\033[0m")
        elif found_info['found']:
            print(f"\033[92m✅ PRIVATE KEY FOUND!\033[0m")
        else:
            print(f"\033[93m❌ Private key not found in this batch\033[0m")
        
        if found_info['speed_info']:
            print(f"\n📈 Performance: {found_info['speed_info']}")
        
        if found_info['found'] or found_info['found_count'] > 0:
            print(f"\n📋 Found information:")
            if found_info['raw_output']:
                for line in found_info['raw_output'].split('\n'):
                    if 'found:' in line.lower() or 'priv' in line.lower():
                        print(f"\033[92m   {line}\033[0m")
                    else:
                        print(f"   {line}")
            else:
                if found_info['private_key_hex']:
                    print(f"   Priv (HEX): \033[92m{found_info['private_key_hex']}\033[0m")
                if found_info['private_key_wif']:
                    print(f"   Priv (WIF): \033[92m{found_info['private_key_wif']}\033[0m")
                if found_info['address']:
                    print(f"   Address: \033[92m{found_info['address']}\033[0m")
                if found_info['wif_key']:
                    print(f"   WIF Key (first 60 chars): \033[92m{found_info['wif_key']}\033[0m")
        
        print(f"{'='*80}")
        
        # Tampilkan return code
        if return_code == 0:
            print(f"\n🟢 Process completed successfully (return code: {return_code})")
        else:
            print(f"\n🟡 Process completed with return code: {return_code}")
        
        return return_code, found_info
        
    except KeyboardInterrupt:
        print(f"\n\n{'='*80}")
        print(f"⚠️  STOPPED BY USER INTERRUPT (Ctrl+C) - GPU {gpu_id}")
        print(f"{'='*80}")
        
        # Update status jika batch diinterupsi
        if batch_id is not None:
            update_batch_status(batch_id, 'interrupted')
        
        return 130, {'found': False}
    except Exception as e:
        error_msg = str(e)
        print(f"\n{'='*80}")
        print(f"❌ ERROR OCCURRED - GPU {gpu_id}")
        print(f"{'='*80}")
        print(f"Error: {error_msg}")
        print(f"{'='*80}")
        
        # Update status error jika ada batch_id
        if batch_id is not None:
            update_batch_status(batch_id, 'error')
        
        return 1, {'found': False}

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

def run_parallel_batches(gpu_ids, batches, address):
    """Menjalankan multiple batch secara paralel di GPU yang berbeda"""
    global STOP_SEARCH_FLAG
    
    results = []
    
    # Gunakan ThreadPoolExecutor untuk menjalankan batch secara paralel
    with ThreadPoolExecutor(max_workers=len(gpu_ids)) as executor:
        # Submit semua batch ke executor
        future_to_batch = {}
        
        for i, batch in enumerate(batches):
            if STOP_SEARCH_FLAG:
                print(f"🚨 Skipping remaining batches due to STOP_SEARCH_FLAG")
                break
                
            gpu_id = gpu_ids[i % len(gpu_ids)]  # Round-robin assignment
            batch_id = batch['id']
            start_range = batch['start_range']
            end_range = batch['end_range']
            
            # Hitung range bits untuk batch ini
            range_bits = calculate_range_bits(start_range, end_range)
            
            print(f"\n📋 Scheduling Batch {batch_id} on GPU {gpu_id}")
            print(f"   Start: {start_range}")
            print(f"   End: {end_range}")
            print(f"   Bits: {range_bits}")
            
            # Submit batch untuk dieksekusi
            future = executor.submit(
                run_xiebo,
                gpu_id, start_range, range_bits, address, batch_id
            )
            future_to_batch[future] = {
                'gpu_id': gpu_id,
                'batch_id': batch_id,
                'batch': batch
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

def run_sequential_batches(gpu_ids, batches, address):
    """Menjalankan batch secara sequential dengan round-robin GPU assignment"""
    global STOP_SEARCH_FLAG
    
    results = []
    
    for i, batch in enumerate(batches):
        if STOP_SEARCH_FLAG:
            print(f"\n🚨 AUTO-STOP TRIGGERED! Stopping remaining batches")
            break
            
        batch_id = batch['id']
        start_range = batch['start_range']
        end_range = batch['end_range']
        
        # Hitung range bits untuk batch ini
        range_bits = calculate_range_bits(start_range, end_range)
        
        # Round-robin GPU assignment
        gpu_id = gpu_ids[i % len(gpu_ids)]
        
        print(f"\n{'='*80}")
        print(f"▶️  BATCH {i+1}/{len(batches)} (Sequential)")
        print(f"{'='*80}")
        print(f"GPU: {gpu_id}")
        print(f"Batch ID: {batch_id}")
        print(f"Start: {start_range}")
        print(f"End: {end_range}")
        print(f"Bits: {range_bits}")
        
        return_code, found_info = run_xiebo(gpu_id, start_range, range_bits, address, batch_id=batch_id)
        
        results.append({
            'gpu_id': gpu_id,
            'batch_id': batch_id,
            'return_code': return_code,
            'found_info': found_info
        })
        
        if return_code == 0:
            print(f"✅ Batch {batch_id} completed successfully")
        else:
            print(f"⚠️  Batch {batch_id} exited with code {return_code}")
        
        # Tampilkan progress
        if (i + 1) % 5 == 0 or i == len(batches) - 1:
            print(f"\n📈 Progress: {i+1}/{len(batches)} batches processed")
        
        # Delay antara batch
        if i < len(batches) - 1 and not STOP_SEARCH_FLAG:
            print(f"\n⏱️  Waiting 3 seconds before next batch...")
            time.sleep(3)
    
    return results

def process_batches_db_parallel(gpu_ids, start_id, address):
    """Proses batch dari database secara paralel dengan multi-GPU"""
    global STOP_SEARCH_FLAG, MAX_BATCHES_PER_RUN
    
    print(f"\n{'='*80}")
    print(f"🚀 PARALLEL MODE - DATABASE DRIVEN")
    print(f"{'='*80}")
    print(f"GPUs: {gpu_ids}")
    print(f"GPU Count: {len(gpu_ids)}")
    print(f"Start ID: {start_id}")
    print(f"Address: {address}")
    print(f"Max batches per run: {MAX_BATCHES_PER_RUN}")
    print(f"Parallel execution: YES")
    print(f"{'='*80}")
    
    # Ambil batch yang pending
    print(f"\n📋 Fetching pending batches from database...")
    batches = get_pending_batches(start_id, MAX_BATCHES_PER_RUN)
    
    if not batches:
        print(f"❌ No pending batches found starting from ID {start_id}")
        return []
    
    print(f"✅ Found {len(batches)} pending batches")
    
    # Jalankan batch secara paralel
    results = run_parallel_batches(gpu_ids, batches, address)
    
    return results

def process_batches_db_sequential(gpu_ids, start_id, address):
    """Proses batch dari database secara sequential dengan multi-GPU"""
    global STOP_SEARCH_FLAG, MAX_BATCHES_PER_RUN
    
    print(f"\n{'='*80}")
    print(f"🚀 SEQUENTIAL MODE - DATABASE DRIVEN")
    print(f"{'='*80}")
    print(f"GPUs: {gpu_ids} (Round-robin assignment)")
    print(f"Start ID: {start_id}")
    print(f"Address: {address}")
    print(f"Max batches per run: {MAX_BATCHES_PER_RUN}")
    print(f"Parallel execution: NO (sequential with GPU round-robin)")
    print(f"{'='*80}")
    
    # Ambil batch yang pending
    print(f"\n📋 Fetching pending batches from database...")
    batches = get_pending_batches(start_id, MAX_BATCHES_PER_RUN)
    
    if not batches:
        print(f"❌ No pending batches found starting from ID {start_id}")
        return []
    
    print(f"✅ Found {len(batches)} pending batches")
    
    # Jalankan batch secara sequential dengan round-robin GPU
    results = run_sequential_batches(gpu_ids, batches, address)
    
    return results

def main():
    global STOP_SEARCH_FLAG
    
    # Reset flag stop search setiap kali program dijalankan
    STOP_SEARCH_FLAG = False
    
    # Parse arguments
    if len(sys.argv) < 2:
        print("Xiebo Batch Runner with SQL Server Database & Multi-GPU Support")
        print("Usage:")
        print("  Single run: python3 bmdb.py GPU_ID START_HEX RANGE_BITS ADDRESS")
        print("  Batch parallel from DB: python3 bmdb.py --batch-db-parallel GPU_IDS START_ID ADDRESS")
        print("  Batch sequential from DB: python3 bmdb.py --batch-db-sequential GPU_IDS START_ID ADDRESS")
        print("\n⚠️  FEATURES:")
        print("  - Menggunakan database SQL Server")
        print("  - Baca range dari tabel Tbatch berdasarkan ID")
        print(f"  - Maksimal {MAX_BATCHES_PER_RUN} batch per eksekusi")
        print("  - Multi-GPU support (parallel dan sequential modes)")
        print("  - Auto-stop ketika ditemukan Found: 1 atau lebih")
        print("  - Real-time output display with colors")
        print("  - Continue ke ID berikutnya secara otomatis")
        sys.exit(1)
    
    # Batch parallel run from database mode
    if sys.argv[1] == "--batch-db-parallel" and len(sys.argv) == 5:
        gpu_ids_str = sys.argv[2]
        start_id = int(sys.argv[3])
        address = sys.argv[4]
        
        gpu_ids = parse_gpu_ids(gpu_ids_str)
        
        results = process_batches_db_parallel(gpu_ids, start_id, address)
        
        # Tampilkan summary
        print(f"\n{'='*80}")
        if STOP_SEARCH_FLAG:
            print(f"🎯 SEARCH STOPPED - PRIVATE KEY FOUND!")
        else:
            print(f"✅ PROCESSING COMPLETED")
        print(f"{'='*80}")
        
        successful_batches = sum(1 for r in results if r['return_code'] == 0)
        found_batches = sum(1 for r in results if r['found_info'].get('found_count', 0) > 0 or r['found_info'].get('found', False))
        
        print(f"\n📋 Summary:")
        print(f"  Batches processed: {len(results)}")
        print(f"  Successful batches: {successful_batches}")
        print(f"  Batches with private keys found: {found_batches}")
        
        if STOP_SEARCH_FLAG:
            print(f"\n🔥 PRIVATE KEY FOUND!")
            print(f"   Check database table {TABLE} for details")
        
        return 0 if successful_batches > 0 else 1
    
    # Batch sequential run from database mode
    elif sys.argv[1] == "--batch-db-sequential" and len(sys.argv) == 5:
        gpu_ids_str = sys.argv[2]
        start_id = int(sys.argv[3])
        address = sys.argv[4]
        
        gpu_ids = parse_gpu_ids(gpu_ids_str)
        
        results = process_batches_db_sequential(gpu_ids, start_id, address)
        
        # Tampilkan summary
        print(f"\n{'='*80}")
        if STOP_SEARCH_FLAG:
            print(f"🎯 SEARCH STOPPED - PRIVATE KEY FOUND!")
        else:
            print(f"✅ PROCESSING COMPLETED")
        print(f"{'='*80}")
        
        successful_batches = sum(1 for r in results if r['return_code'] == 0)
        found_batches = sum(1 for r in results if r['found_info'].get('found_count', 0) > 0 or r['found_info'].get('found', False))
        
        print(f"\n📋 Summary:")
        print(f"  Batches processed: {len(results)}")
        print(f"  Successful batches: {successful_batches}")
        print(f"  Batches with private keys found: {found_batches}")
        
        if STOP_SEARCH_FLAG:
            print(f"\n🔥 PRIVATE KEY FOUND!")
            print(f"   Check database table {TABLE} for details")
        
        return 0 if successful_batches > 0 else 1
    
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
        
        return_code, found_info = run_xiebo(gpu_id, start_hex, range_bits, address)
        
        return return_code
    
    else:
        print("Invalid arguments")
        print("Usage: python3 bmdb.py GPU_ID START_HEX RANGE_BITS ADDRESS")
        print("Or:    python3 bmdb.py --batch-db-parallel GPU_IDS START_ID ADDRESS")
        print("Or:    python3 bmdb.py --batch-db-sequential GPU_IDS START_ID ADDRESS")
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
