import subprocess
import sys
import os
import time
import math
import re
import pyodbc
import threading
import queue
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

def display_xiebo_output_real_time(process, gpu_id):
    """Menampilkan output xiebo secara real-time dengan label GPU"""
    print(f"\n\033[94m╔{'═'*78}╗\033[0m")
    print(f"\033[94m║ 🎯 XIEBO OUTPUT GPU-{gpu_id} (REAL-TIME):\033[0m")
    print(f"\033[94m╚{'═'*78}╝\033[0m")
    
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
                    print(f"\033[92m   [GPU-{gpu_id}] {stripped_line}\033[0m")
                elif 'error' in line_lower or 'failed' in line_lower:
                    # Line dengan error (warna merah)
                    print(f"\033[91m   [GPU-{gpu_id}] {stripped_line}\033[0m")
                elif 'speed' in line_lower or 'key/s' in line_lower:
                    # Line dengan informasi speed (warna kuning)
                    print(f"\033[93m   [GPU-{gpu_id}] {stripped_line}\033[0m")
                elif 'range' in line_lower:
                    # Line dengan informasi range (warna biru)
                    print(f"\033[94m   [GPU-{gpu_id}] {stripped_line}\033[0m")
                else:
                    # Line normal (warna default)
                    print(f"   [GPU-{gpu_id}] {stripped_line}")
            output_lines.append(output_line)
    
    output_text = ''.join(output_lines)
    print(f"\033[94m╔{'═'*78}╗\033[0m")
    print(f"\033[94m║ 🎯 END OF GPU-{gpu_id} OUTPUT\033[0m")
    print(f"\033[94m╚{'═'*78}╝\033[0m")
    
    return output_text

def run_xiebo(gpu_id, start_hex, range_bits, address, batch_id=None):
    """Run xiebo binary langsung dan tampilkan outputnya secara real-time"""
    global STOP_SEARCH_FLAG
    
    cmd = ["./xiebo", "-gpuId", str(gpu_id), "-start", start_hex, 
           "-range", str(range_bits), address]
    
    print(f"\n\033[93m╔{'═'*78}╗\033[0m")
    print(f"\033[93m║ 🚀 STARTING XIEBO EXECUTION GPU-{gpu_id}\033[0m")
    print(f"\033[93m╚{'═'*78}╝\033[0m")
    print(f"Command: {' '.join(cmd)}")
    print(f"Batch ID: {batch_id if batch_id is not None else 'N/A'}")
    print(f"GPU ID: {gpu_id}")
    print(f"Start Hex: {start_hex}")
    print(f"Range Bits: {range_bits}")
    
    try:
        # Update status menjadi inprogress jika ada batch_id
        if batch_id is not None:
            update_batch_status(batch_id, 'inprogress')
        
        # Jalankan xiebo dan tampilkan output secara real-time
        print(f"\n⏳ Launching xiebo process for GPU-{gpu_id}...")
        
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
        print(f"\n\033[93m╔{'═'*78}╗\033[0m")
        print(f"\033[93m║ 📊 SEARCH RESULT SUMMARY GPU-{gpu_id}\033[0m")
        print(f"\033[93m╚{'═'*78}╝\033[0m")
        
        if found_info['found_count'] > 0:
            print(f"\033[92m✅ GPU-{gpu_id} FOUND: {found_info['found_count']} PRIVATE KEY(S)!\033[0m")
        elif found_info['found']:
            print(f"\033[92m✅ GPU-{gpu_id} PRIVATE KEY FOUND!\033[0m")
        else:
            print(f"\033[93m❌ GPU-{gpu_id}: Private key not found in this batch\033[0m")
        
        if found_info['speed_info']:
            print(f"\n📈 GPU-{gpu_id} Performance: {found_info['speed_info']}")
        
        if found_info['found'] or found_info['found_count'] > 0:
            print(f"\n📋 GPU-{gpu_id} Found information:")
            if found_info['raw_output']:
                for line in found_info['raw_output'].split('\n'):
                    if 'found:' in line.lower() or 'priv' in line.lower():
                        print(f"\033[92m   [GPU-{gpu_id}] {line}\033[0m")
                    else:
                        print(f"   [GPU-{gpu_id}] {line}")
            else:
                if found_info['private_key_hex']:
                    print(f"   [GPU-{gpu_id}] Priv (HEX): \033[92m{found_info['private_key_hex']}\033[0m")
                if found_info['private_key_wif']:
                    print(f"   [GPU-{gpu_id}] Priv (WIF): \033[92m{found_info['private_key_wif']}\033[0m")
                if found_info['address']:
                    print(f"   [GPU-{gpu_id}] Address: \033[92m{found_info['address']}\033[0m")
                if found_info['wif_key']:
                    print(f"   [GPU-{gpu_id}] WIF Key (first 60 chars): \033[92m{found_info['wif_key']}\033[0m")
        
        # Tampilkan return code
        if return_code == 0:
            print(f"\n🟢 GPU-{gpu_id} process completed successfully (return code: {return_code})")
        else:
            print(f"\n🟡 GPU-{gpu_id} process completed with return code: {return_code}")
        
        return return_code, found_info
        
    except KeyboardInterrupt:
        print(f"\n\n\033[91m╔{'═'*78}╗\033[0m")
        print(f"\033[91m║ ⚠️  GPU-{gpu_id} STOPPED BY USER INTERRUPT (Ctrl+C)\033[0m")
        print(f"\033[91m╚{'═'*78}╝\033[0m")
        
        # Update status jika batch diinterupsi
        if batch_id is not None:
            update_batch_status(batch_id, 'interrupted')
        
        return 130, {'found': False}
    except Exception as e:
        error_msg = str(e)
        print(f"\n\033[91m╔{'═'*78}╗\033[0m")
        print(f"\033[91m║ ❌ GPU-{gpu_id} ERROR OCCURRED\033[0m")
        print(f"\033[91m╚{'═'*78}╝\033[0m")
        print(f"Error: {error_msg}")
        
        # Update status error jika ada batch_id
        if batch_id is not None:
            update_batch_status(batch_id, 'error')
        
        return 1, {'found': False}

def process_gpu_batch(gpu_config):
    """Fungsi untuk memproses batch pada GPU tertentu"""
    global STOP_SEARCH_FLAG
    
    gpu_id = gpu_config['gpu_id']
    start_id = gpu_config['start_id']
    address = gpu_config['address']
    max_batches = gpu_config.get('max_batches', MAX_BATCHES_PER_RUN)
    
    current_id = start_id
    batches_processed = 0
    
    print(f"\n\033[96m╔{'═'*78}╗\033[0m")
    print(f"\033[96m║ 🚀 STARTING GPU-{gpu_id} BATCH PROCESSING\033[0m")
    print(f"\033[96m╚{'═'*78}╝\033[0m")
    print(f"GPU: {gpu_id}")
    print(f"Start ID: {start_id}")
    print(f"Address: {address}")
    print(f"Max batches: {max_batches}")
    
    # Loop untuk memproses batch secara berurutan
    while batches_processed < max_batches and not STOP_SEARCH_FLAG:
        print(f"\n📋 GPU-{gpu_id} processing batch ID: {current_id}")
        
        # Ambil data batch berdasarkan ID
        batch = get_batch_by_id(current_id)
        
        if not batch:
            print(f"❌ GPU-{gpu_id}: Batch ID {current_id} not found in database. Stopping.")
            break
        
        # Cek status batch
        status = batch.get('status', '').strip()
        
        if status == 'done':
            print(f"⏭️  GPU-{gpu_id}: Batch ID {current_id} already done. Skipping to next ID.")
            current_id += 1
            continue
        
        if status == 'inprogress':
            print(f"⏭️  GPU-{gpu_id}: Batch ID {current_id} is in progress. Skipping to next ID.")
            current_id += 1
            continue
        
        # Ambil data range
        start_range = batch['start_range']
        end_range = batch['end_range']
        
        # Hitung range bits
        range_bits = calculate_range_bits(start_range, end_range)
        
        # Run batch
        print(f"\n\033[96m╔{'═'*78}╗\033[0m")
        print(f"\033[96m║ ▶️  GPU-{gpu_id} BATCH {batches_processed + 1} (ID: {current_id})\033[0m")
        print(f"\033[96m╚{'═'*78}╝\033[0m")
        print(f"GPU: {gpu_id}")
        print(f"Start Range: {start_range}")
        print(f"End Range: {end_range}")
        print(f"Range Bits: {range_bits}")
        print(f"Address: {address}")
        
        return_code, found_info = run_xiebo(gpu_id, start_range, range_bits, address, batch_id=current_id)
        
        if return_code == 0:
            print(f"\n✅ GPU-{gpu_id}: Batch ID {current_id} completed successfully")
        else:
            print(f"\n⚠️  GPU-{gpu_id}: Batch ID {current_id} exited with code {return_code}")
        
        # Increment counters
        batches_processed += 1
        current_id += 1
        
        # Tampilkan progress
        if batches_processed % 3 == 0 or STOP_SEARCH_FLAG:
            print(f"\n📈 GPU-{gpu_id} Progress: {batches_processed} batches processed, current ID: {current_id}")
        
        # Delay antara batch (kecuali jika STOP_SEARCH_FLAG aktif)
        if not STOP_SEARCH_FLAG and batches_processed < max_batches:
            print(f"\n⏱️  GPU-{gpu_id} waiting 2 seconds before next batch...")
            time.sleep(2)
    
    return {
        'gpu_id': gpu_id,
        'batches_processed': batches_processed,
        'last_processed_id': current_id - 1,
        'next_id': current_id,
        'found': STOP_SEARCH_FLAG
    }

def main():
    global STOP_SEARCH_FLAG
    
    # Reset flag stop search setiap kali program dijalankan
    STOP_SEARCH_FLAG = False
    
    # Parse arguments
    if len(sys.argv) < 2:
        print("Xiebo Batch Runner with SQL Server Database")
        print("Usage:")
        print("  Single run: python3 bm.py GPU_ID START_HEX RANGE_BITS ADDRESS")
        print("  Batch run from DB: python3 bm.py --batch-db GPU_ID START_ID ADDRESS")
        print("  Multi-GPU batch run: python3 bm.py --batch-multi-gpu GPU_CONFIG1,GPU_CONFIG2,... ADDRESS")
        print("     GPU_CONFIG format: gpu_id:start_id")
        print("     Example: 0:1,1:100,2:200")
        print("\n⚠️  FEATURES:")
        print("  - Menggunakan database SQL Server")
        print("  - Baca range dari tabel Tbatch berdasarkan ID")
        print(f"  - Maksimal {MAX_BATCHES_PER_RUN} batch per eksekusi")
        print("  - Multi-GPU support dengan range berbeda per GPU")
        print("  - Auto-stop ketika ditemukan Found: 1 atau lebih")
        print("  - Real-time output display with colors")
        print("  - Continue ke ID berikutnya secara otomatis")
        sys.exit(1)
    
    # Multi-GPU batch run mode
    if sys.argv[1] == "--batch-multi-gpu" and len(sys.argv) == 4:
        gpu_configs_str = sys.argv[2]
        address = sys.argv[3]
        
        # Parse GPU configurations
        gpu_configs = []
        try:
            configs = gpu_configs_str.split(',')
            for config in configs:
                if ':' in config:
                    gpu_id, start_id = config.split(':')
                    gpu_configs.append({
                        'gpu_id': gpu_id.strip(),
                        'start_id': int(start_id.strip()),
                        'address': address
                    })
                else:
                    print(f"⚠️  Invalid GPU config format: {config}. Using default start ID 1")
                    gpu_configs.append({
                        'gpu_id': config.strip(),
                        'start_id': 1,
                        'address': address
                    })
        except Exception as e:
            print(f"❌ Error parsing GPU configurations: {e}")
            sys.exit(1)
        
        print(f"\n\033[95m╔{'═'*78}╗\033[0m")
        print(f"\033[95m║ 🚀 MULTI-GPU BATCH MODE - {len(gpu_configs)} GPUs\033[0m")
        print(f"\033[95m╚{'═'*78}╝\033[0m")
        print(f"Address: {address}")
        print(f"Total GPUs: {len(gpu_configs)}")
        for i, config in enumerate(gpu_configs):
            print(f"  GPU-{config['gpu_id']}: Start ID = {config['start_id']}")
        print(f"Max batches per GPU: {MAX_BATCHES_PER_RUN}")
        
        # Jalankan setiap GPU dalam thread terpisah
        results = []
        with ThreadPoolExecutor(max_workers=len(gpu_configs)) as executor:
            # Submit semua GPU tasks
            future_to_gpu = {executor.submit(process_gpu_batch, config): config for config in gpu_configs}
            
            # Tunggu semua task selesai
            for future in as_completed(future_to_gpu):
                config = future_to_gpu[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    print(f"❌ GPU-{config['gpu_id']} error: {e}")
                    results.append({
                        'gpu_id': config['gpu_id'],
                        'batches_processed': 0,
                        'error': str(e)
                    })
        
        # Tampilkan summary semua GPU
        print(f"\n\033[95m╔{'═'*78}╗\033[0m")
        print(f"\033[95m║ 📊 MULTI-GPU SUMMARY\033[0m")
        print(f"\033[95m╚{'═'*78}╝\033[0m")
        
        total_batches = 0
        for result in results:
            gpu_id = result.get('gpu_id', 'N/A')
            batches = result.get('batches_processed', 0)
            last_id = result.get('last_processed_id', 'N/A')
            next_id = result.get('next_id', 'N/A')
            
            total_batches += batches
            
            if 'error' in result:
                print(f"❌ GPU-{gpu_id}: ERROR - {result['error']}")
            else:
                print(f"✅ GPU-{gpu_id}: Processed {batches} batches")
                print(f"   Last processed ID: {last_id}")
                print(f"   Next ID to process: {next_id}")
        
        print(f"\n📈 TOTAL: {total_batches} batches processed across {len(gpu_configs)} GPUs")
        
        if STOP_SEARCH_FLAG:
            print(f"\n\033[92m🔥 PRIVATE KEY FOUND IN ONE OR MORE GPUS!\033[0m")
            print(f"   Check database table Tbatch for details")
        
        return 0
    
    # Batch run from database mode (single GPU)
    elif sys.argv[1] == "--batch-db" and len(sys.argv) == 5:
        gpu_id = sys.argv[2]
        start_id = int(sys.argv[3])
        address = sys.argv[4]
        
        # Gunakan fungsi process_gpu_batch untuk konsistensi
        result = process_gpu_batch({
            'gpu_id': gpu_id,
            'start_id': start_id,
            'address': address
        })
        
        print(f"\n\033[95m╔{'═'*78}╗\033[0m")
        if STOP_SEARCH_FLAG:
            print(f"\033[95m║ 🎯 SEARCH STOPPED - PRIVATE KEY FOUND!\033[0m")
        else:
            print(f"\033[95m║ ✅ PROCESSING COMPLETED\033[0m")
        print(f"\033[95m╚{'═'*78}╝\033[0m")
        
        print(f"\n📋 Summary:")
        print(f"  GPU: {gpu_id}")
        print(f"  Start ID: {start_id}")
        print(f"  Last processed ID: {result.get('last_processed_id', 'N/A')}")
        print(f"  Batches processed: {result.get('batches_processed', 0)}")
        print(f"  Next ID to process: {result.get('next_id', 'N/A')}")
        
        if STOP_SEARCH_FLAG:
            print(f"\n🔥 PRIVATE KEY FOUND!")
            print(f"   Check database table Tbatch for details")
    
    # Single run mode (tetap support untuk backward compatibility)
    elif len(sys.argv) == 5:
        gpu_id = sys.argv[1]
        start_hex = sys.argv[2]
        range_bits = int(sys.argv[3])
        address = sys.argv[4]
        
        print(f"\n\033[95m╔{'═'*78}╗\033[0m")
        print(f"\033[95m║ 🚀 SINGLE RUN MODE\033[0m")
        print(f"\033[95m╚{'═'*78}╝\033[0m")
        print(f"GPU: {gpu_id}")
        print(f"Start: 0x{start_hex}")
        print(f"Range: {range_bits} bits")
        print(f"Address: {address}")
        
        return_code, found_info = run_xiebo(gpu_id, start_hex, range_bits, address)
        
        return return_code
    
    else:
        print("Invalid arguments")
        print("Usage:")
        print("  Single run: python3 bm.py GPU_ID START_HEX RANGE_BITS ADDRESS")
        print("  Batch run from DB: python3 bm.py --batch-db GPU_ID START_ID ADDRESS")
        print("  Multi-GPU batch run: python3 bm.py --batch-multi-gpu GPU_CONFIGS ADDRESS")
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
