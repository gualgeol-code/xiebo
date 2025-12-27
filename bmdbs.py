import subprocess
import sys
import os
import time
import math
import re
import pyodbc

# Konfigurasi database SQL Server
SERVER = "benilapo-31088.portmap.host,31088"
DATABASE = "puxi"
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

def display_xiebo_output_real_time(process):
    """Menampilkan output xiebo secara real-time"""
    print("\n" + "─" * 80)
    print("🎯 XIEBO OUTPUT (REAL-TIME):")
    print("─" * 80)
    
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
                    print(f"\033[92m   {stripped_line}\033[0m")
                elif 'error' in line_lower or 'failed' in line_lower:
                    # Line dengan error (warna merah)
                    print(f"\033[91m   {stripped_line}\033[0m")
                elif 'speed' in line_lower or 'key/s' in line_lower:
                    # Line dengan informasi speed (warna kuning)
                    print(f"\033[93m   {stripped_line}\033[0m")
                elif 'range' in line_lower:
                    # Line dengan informasi range (warna biru)
                    print(f"\033[94m   {stripped_line}\033[0m")
                else:
                    # Line normal (warna default)
                    print(f"   {stripped_line}")
            output_lines.append(output_line)
    
    output_text = ''.join(output_lines)
    print("─" * 80)
    
    return output_text

def run_xiebo(gpu_id, start_hex, range_bits, address, batch_id=None):
    """Run xiebo binary langsung dan tampilkan outputnya secara real-time"""
    global STOP_SEARCH_FLAG
    
    cmd = ["./xiebo", "-gpuId", str(gpu_id), "-start", start_hex, 
           "-range", str(range_bits), address]
    
    print(f"\n{'='*80}")
    print(f"🚀 STARTING XIEBO EXECUTION")
    print(f"{'='*80}")
    print(f"Command: {' '.join(cmd)}")
    print(f"Batch ID: {batch_id if batch_id is not None else 'N/A'}")
    print(f"{'='*80}")
    
    try:
        # Update status menjadi inprogress jika ada batch_id
        if batch_id is not None:
            update_batch_status(batch_id, 'inprogress')
        
        # Jalankan xiebo dan tampilkan output secara real-time
        print(f"\n⏳ Launching xiebo process...")
        
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
        output_text = display_xiebo_output_real_time(process)
        
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
        print(f"📊 SEARCH RESULT SUMMARY")
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
        print(f"⚠️  STOPPED BY USER INTERRUPT (Ctrl+C)")
        print(f"{'='*80}")
        
        # Update status jika batch diinterupsi
        if batch_id is not None:
            update_batch_status(batch_id, 'interrupted')
        
        return 130, {'found': False}
    except Exception as e:
        error_msg = str(e)
        print(f"\n{'='*80}")
        print(f"❌ ERROR OCCURRED")
        print(f"{'='*80}")
        print(f"Error: {error_msg}")
        print(f"{'='*80}")
        
        # Update status error jika ada batch_id
        if batch_id is not None:
            update_batch_status(batch_id, 'error')
        
        return 1, {'found': False}

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
        print("\n⚠️  FEATURES:")
        print("  - Menggunakan database SQL Server")
        print("  - Baca range dari tabel Tbatch berdasarkan ID")
        print(f"  - Maksimal {MAX_BATCHES_PER_RUN} batch per eksekusi")
        print("  - Auto-stop ketika ditemukan Found: 1 atau lebih")
        print("  - Real-time output display with colors")
        print("  - Continue ke ID berikutnya secara otomatis")
        sys.exit(1)
    
    # Batch run from database mode
    if sys.argv[1] == "--batch-db" and len(sys.argv) == 5:
        gpu_id = sys.argv[2]
        start_id = int(sys.argv[3])
        address = sys.argv[4]
        
        print(f"\n{'='*80}")
        print(f"🚀 BATCH MODE - DATABASE DRIVEN")
        print(f"{'='*80}")
        print(f"GPU: {gpu_id}")
        print(f"Start ID: {start_id}")
        print(f"Address: {address}")
        print(f"Max batches per run: {MAX_BATCHES_PER_RUN}")
        print(f"{'='*80}")
        
        current_id = start_id
        batches_processed = 0
        
        # Loop untuk memproses batch secara berurutan
        while batches_processed < MAX_BATCHES_PER_RUN and not STOP_SEARCH_FLAG:
            print(f"\n📋 Processing batch ID: {current_id}")
            
            # Ambil data batch berdasarkan ID
            batch = get_batch_by_id(current_id)
            
            if not batch:
                print(f"❌ Batch ID {current_id} not found in database. Stopping.")
                break
            
            # Cek status batch
            status = batch.get('status', '').strip()
            
            if status == 'done':
                print(f"⏭️  Batch ID {current_id} already done. Skipping to next ID.")
                current_id += 1
                continue
            
            if status == 'inprogress':
                print(f"⏭️  Batch ID {current_id} is in progress. Skipping to next ID.")
                current_id += 1
                continue
            
            # Ambil data range
            start_range = batch['start_range']
            end_range = batch['end_range']
            
            # Hitung range bits
            range_bits = calculate_range_bits(start_range, end_range)
            
            # Run batch
            print(f"\n{'='*80}")
            print(f"▶️  BATCH {batches_processed + 1} (ID: {current_id})")
            print(f"{'='*80}")
            print(f"Start Range: {start_range}")
            print(f"End Range: {end_range}")
            print(f"Range Bits: {range_bits}")
            print(f"Address: {address}")
            print(f"{'='*80}")
            
            return_code, found_info = run_xiebo(gpu_id, start_range, range_bits, address, batch_id=current_id)
            
            if return_code == 0:
                print(f"\n✅ Batch ID {current_id} completed successfully")
            else:
                print(f"\n⚠️  Batch ID {current_id} exited with code {return_code}")
            
            # Increment counters
            batches_processed += 1
            current_id += 1
            
            # Tampilkan progress
            if batches_processed % 5 == 0 or STOP_SEARCH_FLAG:
                print(f"\n📈 Progress: {batches_processed} batches processed, current ID: {current_id}")
            
            # Delay antara batch (kecuali jika STOP_SEARCH_FLAG aktif)
            if not STOP_SEARCH_FLAG and batches_processed < MAX_BATCHES_PER_RUN:
                print(f"\n⏱️  Waiting 3 seconds before next batch...")
                time.sleep(3)
        
        print(f"\n{'='*80}")
        if STOP_SEARCH_FLAG:
            print(f"🎯 SEARCH STOPPED - PRIVATE KEY FOUND!")
        elif batches_processed >= MAX_BATCHES_PER_RUN:
            print(f"⏹️  MAX BATCHES REACHED - Processed {batches_processed} batches")
        else:
            print(f"✅ PROCESSING COMPLETED - Processed {batches_processed} batches")
        print(f"{'='*80}")
        
        print(f"\n📋 Summary:")
        print(f"  Start ID: {start_id}")
        print(f"  Last processed ID: {current_id - 1}")
        print(f"  Batches processed: {batches_processed}")
        print(f"  Next ID to process: {current_id}")
        
        if STOP_SEARCH_FLAG:
            print(f"\n🔥 PRIVATE KEY FOUND!")
            print(f"   Check database table Tbatch for details")
        
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
        print("Usage: python3 bm.py GPU_ID START_HEX RANGE_BITS ADDRESS")
        print("Or:    python3 bm.py --batch-db GPU_ID START_ID ADDRESS")
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
