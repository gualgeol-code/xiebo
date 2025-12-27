import subprocess
import sys
import os
import time
import math
import re
import pyodbc
import threading
from datetime import datetime

# Konfigurasi database SQL Server
SERVER = "benilapo-31088.portmap.host,31088"
DATABASE = "puxi"
USERNAME = "sa"
PASSWORD = "LEtoy_89"
TABLE = "dbo.Tbatch"

# Global flag untuk menghentikan pencarian
STOP_SEARCH_FLAG = False

# Global variables untuk Threading synchronization
PRINT_LOCK = threading.Lock()
BATCH_ID_LOCK = threading.Lock()
CURRENT_GLOBAL_BATCH_ID = 0

# Konfigurasi batch
MAX_BATCHES_PER_RUN = 4000000000000  # Maksimal batch per eksekusi

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
        safe_print(f"❌ Database connection error: {e}")
        return None

def safe_print(message):
    """Mencetak pesan ke layar dengan thread lock agar tidak tumpang tindih"""
    with PRINT_LOCK:
        print(message)

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
        safe_print(f"❌ Error getting batch by ID: {e}")
        if conn:
            conn.close()
        return None

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
        
        # safe_print(f"📝 Updated batch {batch_id}: status={status}, found={found}")
        return True
        
    except Exception as e:
        safe_print(f"❌ Error updating batch status: {e}")
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
        
        if log2_val.is_integer():
            return int(log2_val)
        else:
            return int(math.floor(log2_val)) + 1
            
    except Exception as e:
        safe_print(f"❌ Error calculating range bits: {e}")
        return 64  # Default value

def parse_xiebo_output(output_text, gpu_prefix=""):
    """Parse output dari xiebo untuk mencari private key yang ditemukan"""
    global STOP_SEARCH_FLAG
    
    found_info = {
        'found': False,
        'found_count': 0,
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
        
        # 1. Cari pattern "Found: X"
        if 'range finished!' in line_lower and 'found:' in line_lower:
            found_match = re.search(r'found:\s*(\d+)', line_lower)
            if found_match:
                found_count = int(found_match.group(1))
                found_info['found_count'] = found_count
                found_info['found'] = found_count > 0
                found_info['speed_info'] = line_stripped
                found_lines.append(line_stripped)
                
                if found_count >= 1:
                    STOP_SEARCH_FLAG = True
                    safe_print(f"{gpu_prefix} 🚨 STOP_SEARCH_FLAG diaktifkan karena Found: {found_count}")
        
        # 2. Cari pattern Priv (HEX)
        elif 'priv (hex):' in line_lower:
            found_info['found'] = True
            found_info['private_key_hex'] = line_stripped.replace('Priv (HEX):', '').replace('Priv (hex):', '').strip()
            found_lines.append(line_stripped)
        
        # 3. Cari pattern Priv (WIF)
        elif 'priv (wif):' in line_lower:
            found_info['found'] = True
            wif_value = line_stripped.replace('Priv (WIF):', '').replace('Priv (wif):', '').strip()
            found_info['private_key_wif'] = wif_value
            if len(wif_value) >= 60:
                found_info['wif_key'] = wif_value[:60]
            else:
                found_info['wif_key'] = wif_value
            found_lines.append(line_stripped)
        
        # 4. Cari pattern Address
        elif 'address:' in line_lower and found_info['found']:
            found_info['address'] = line_stripped.replace('Address:', '').replace('address:', '').strip()
            found_lines.append(line_stripped)
        
        # 5. Cari pattern lain
        elif any(keyword in line_lower for keyword in ['found', 'success', 'match']) and 'private' in line_lower:
            found_info['found'] = True
            found_lines.append(line_stripped)
    
    if found_lines:
        found_info['raw_output'] = '\n'.join(found_lines)
        # Fallback logic untuk wif_key
        if found_info['private_key_wif'] and not found_info['wif_key']:
            found_info['wif_key'] = found_info['private_key_wif'][:60]
        elif found_info['private_key_hex'] and not found_info['wif_key']:
            found_info['wif_key'] = found_info['private_key_hex'][:60]
    
    return found_info

def display_xiebo_output_real_time(process, gpu_id):
    """Menampilkan output xiebo secara real-time dengan prefix GPU ID"""
    gpu_prefix = f"\033[96m[GPU {gpu_id}]\033[0m"
    
    output_lines = []
    while True:
        output_line = process.stdout.readline()
        if output_line == '' and process.poll() is not None:
            break
        if output_line:
            stripped_line = output_line.strip()
            if stripped_line:
                line_lower = stripped_line.lower()
                
                # Filter output agar tidak terlalu spam di multi-gpu, 
                # kecuali info penting atau speed
                should_print = False
                color_code = ""

                if 'found:' in line_lower or 'success' in line_lower:
                    color_code = "\033[92m" # Hijau
                    should_print = True
                elif 'error' in line_lower or 'failed' in line_lower:
                    color_code = "\033[91m" # Merah
                    should_print = True
                elif 'speed' in line_lower or 'key/s' in line_lower:
                    color_code = "\033[93m" # Kuning
                    # Di multi-gpu, mungkin kita print speed sesekali saja atau tetap print
                    should_print = True 
                elif 'range' in line_lower:
                    color_code = "\033[94m" # Biru
                    should_print = True
                
                if should_print:
                    safe_print(f"{gpu_prefix} {color_code}{stripped_line}\033[0m")
            
            output_lines.append(output_line)
    
    output_text = ''.join(output_lines)
    return output_text

def run_xiebo(gpu_id, start_hex, range_bits, address, batch_id=None):
    """Run xiebo binary langsung"""
    global STOP_SEARCH_FLAG
    
    cmd = ["./xiebo", "-gpuId", str(gpu_id), "-start", start_hex, 
           "-range", str(range_bits), address]
    
    gpu_prefix = f"[GPU {gpu_id}]"
    
    # Gunakan lock hanya untuk print block besar ini agar rapi
    with PRINT_LOCK:
        print(f"\n{gpu_prefix} {'='*60}")
        print(f"{gpu_prefix} 🚀 EXECUTION START | Batch: {batch_id}")
        print(f"{gpu_prefix} Command: {' '.join(cmd)}")
        print(f"{gpu_prefix} {'='*60}")
    
    try:
        if batch_id is not None:
            update_batch_status(batch_id, 'inprogress')
        
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        # Pass gpu_id ke display function
        output_text = display_xiebo_output_real_time(process, gpu_id)
        
        return_code = process.wait()
        found_info = parse_xiebo_output(output_text, gpu_prefix)
        
        if batch_id is not None:
            found_status = 'Yes' if (found_info['found_count'] > 0 or found_info['found']) else 'No'
            wif_key = found_info['wif_key'] if found_info['wif_key'] else ''
            update_batch_status(batch_id, 'done', found_status, wif_key)
        
        # Summary Print
        with PRINT_LOCK:
            if found_info['found'] or found_info['found_count'] > 0:
                print(f"\n{gpu_prefix} \033[92m✅ FOUND PRIVATE KEY IN BATCH {batch_id}!\033[0m")
                if found_info['private_key_wif']:
                    print(f"{gpu_prefix} WIF: {found_info['private_key_wif']}")
                if found_info['private_key_hex']:
                    print(f"{gpu_prefix} HEX: {found_info['private_key_hex']}")
            else:
                # Optional: Uncomment to see completed msg per batch
                # print(f"{gpu_prefix} Batch {batch_id} completed (Not Found).")
                pass

        return return_code, found_info
        
    except KeyboardInterrupt:
        safe_print(f"\n{gpu_prefix} ⚠️ Process Interrupted")
        if batch_id is not None:
            update_batch_status(batch_id, 'interrupted')
        return 130, {'found': False}
    except Exception as e:
        safe_print(f"\n{gpu_prefix} ❌ Error: {e}")
        if batch_id is not None:
            update_batch_status(batch_id, 'error')
        return 1, {'found': False}

def gpu_worker(gpu_id, address):
    """Worker function untuk setiap thread GPU"""
    global CURRENT_GLOBAL_BATCH_ID, STOP_SEARCH_FLAG
    
    batches_processed = 0
    
    while not STOP_SEARCH_FLAG:
        # 1. Ambil Batch ID berikutnya secara aman (Thread Safe)
        batch_id_to_process = -1
        with BATCH_ID_LOCK:
            batch_id_to_process = CURRENT_GLOBAL_BATCH_ID
            CURRENT_GLOBAL_BATCH_ID += 1
            
        # Cek batas maksimal batch per run (global check bisa ditambahkan jika perlu)
        # Disini kita loop terus sampai flag stop
        
        # 2. Ambil Data Batch dari DB
        batch = get_batch_by_id(batch_id_to_process)
        
        if not batch:
            safe_print(f"[GPU {gpu_id}] ❌ Batch ID {batch_id_to_process} not found in DB. Worker stopping.")
            break
            
        status = batch.get('status', '').strip()
        
        # Skip jika sudah selesai atau sedang dikerjakan
        if status == 'done' or status == 'inprogress':
            # Jangan print skip terlalu banyak agar log bersih
            if batch_id_to_process % 100 == 0: 
                safe_print(f"[GPU {gpu_id}] Skipping ID {batch_id_to_process} (Status: {status})")
            continue
            
        start_range = batch['start_range']
        end_range = batch['end_range']
        range_bits = calculate_range_bits(start_range, end_range)
        
        # 3. Jalankan Xiebo
        return_code, found_info = run_xiebo(gpu_id, start_range, range_bits, address, batch_id=batch_id_to_process)
        
        batches_processed += 1
        
        # Stop jika error fatal atau user stop
        if STOP_SEARCH_FLAG:
            break
            
        # Delay sedikit antar batch per GPU agar tidak terlalu spam request ke DB/Screen
        time.sleep(1)

    safe_print(f"[GPU {gpu_id}] 🛑 Worker stopped. Processed {batches_processed} batches.")

def main():
    global STOP_SEARCH_FLAG, CURRENT_GLOBAL_BATCH_ID
    
    STOP_SEARCH_FLAG = False
    
    if len(sys.argv) < 2:
        print("Xiebo Multi-GPU Batch Runner")
        print("Usage:")
        print("  Multi-GPU DB: python3 bm.py --batch-db GPU_IDS START_ID ADDRESS")
        print("  Example:      python3 bm.py --batch-db 0,1,2,3 1000 13zpGr...")
        print("  Single Run:   python3 bm.py GPU_ID START_HEX RANGE_BITS ADDRESS")
        sys.exit(1)
    
    # Mode Multi-GPU Database
    if sys.argv[1] == "--batch-db" and len(sys.argv) == 5:
        gpu_ids_str = sys.argv[2]
        start_id = int(sys.argv[3])
        address = sys.argv[4]
        
        # Parse list GPU (misal "0,1,2" -> [0, 1, 2])
        gpu_ids = [int(x.strip()) for x in gpu_ids_str.split(',')]
        
        # Set Global Start ID
        CURRENT_GLOBAL_BATCH_ID = start_id
        
        print(f"\n{'='*80}")
        print(f"🚀 MULTI-GPU BATCH MODE STARTED")
        print(f"{'='*80}")
        print(f"GPUs Active : {gpu_ids}")
        print(f"Start ID    : {start_id}")
        print(f"Address     : {address}")
        print(f"{'='*80}\n")
        
        threads = []
        
        # Buat dan jalankan thread untuk setiap GPU
        for gpu in gpu_ids:
            t = threading.Thread(target=gpu_worker, args=(gpu, address))
            t.daemon = True # Agar thread mati jika main program di kill
            threads.append(t)
            t.start()
            print(f"✅ Started worker thread for GPU {gpu}")
        
        # Main loop untuk menjaga program tetap berjalan dan handle KeyboardInterrupt
        try:
            while True:
                # Cek apakah semua thread masih hidup
                alive_threads = [t for t in threads if t.is_alive()]
                if not alive_threads:
                    print("\nAll workers have finished.")
                    break
                
                if STOP_SEARCH_FLAG:
                    print("\n🛑 Stop Flag Detected. Waiting for workers to finish...")
                    break
                    
                time.sleep(1)
                
            # Wait for all threads
            for t in threads:
                t.join(timeout=5)
                
        except KeyboardInterrupt:
            print(f"\n\n{'='*80}")
            print(f"⚠️  STOPPED BY USER INTERRUPT (Ctrl+C)")
            print(f"{'='*80}")
            STOP_SEARCH_FLAG = True
            # Beri waktu worker untuk cleanup
            time.sleep(2)
            
    # Single run mode (Legacy support)
    elif len(sys.argv) == 5:
        gpu_id = sys.argv[1]
        start_hex = sys.argv[2]
        range_bits = int(sys.argv[3])
        address = sys.argv[4]
        
        run_xiebo(gpu_id, start_hex, range_bits, address)
        
    else:
        print("Invalid arguments")
        print("Usage: python3 bm.py --batch-db 0,1,2 1000 1Address...")
        
if __name__ == "__main__":
    if not os.path.exists("./xiebo"):
        print("❌ Error: xiebo binary not found")
        sys.exit(1)
        
    if not os.access("./xiebo", os.X_OK):
        os.chmod("./xiebo", 0o755)
        
    if os.name == 'posix':
        os.system('')
        
    main()
