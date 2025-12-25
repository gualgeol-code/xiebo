import subprocess
import sys
import os
import time
import math
from datetime import datetime
import csv
import pyodbc

# Konfigurasi database SQL Server
SERVER = "benilapo-31088.portmap.host,31088"
DATABASE = "puzzle"
USERNAME = "sa"
PASSWORD = "LEtoy_89"
TABLE = "dbo.Tbatch"

# Global flag untuk menghentikan pencarian
STOP_SEARCH_FLAG = False

# Konfigurasi batch
MAX_BATCHES_PER_RUN = 100  # Maksimal 1juta batch per eksekusi

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

def initialize_database():
    """Inisialisasi tabel Tbatch jika belum ada"""
    conn = connect_db()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # Cek apakah tabel Tbatch sudah ada
        cursor.execute("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = 'Tbatch' AND TABLE_SCHEMA = 'dbo'
        """)
        
        if cursor.fetchone()[0] == 0:
            # Buat tabel Tbatch
            cursor.execute(f"""
                CREATE TABLE {TABLE} (
                    id INT PRIMARY KEY,
                    start_range VARCHAR(64),
                    end_range VARCHAR(64),
                    status VARCHAR(20) DEFAULT 'uncheck',
                    found VARCHAR(3) DEFAULT '',
                    wif VARCHAR(255) DEFAULT ''
                )
            """)
            conn.commit()
            print("✅ Created Tbatch table")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Error initializing database: {e}")
        if conn:
            conn.rollback()
            conn.close()
        return False

def get_batch_range(start_id):
    """Mengambil batch range dari database berdasarkan ID"""
    conn = connect_db()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        
        # Ambil batch dengan status 'uncheck' dimulai dari ID tertentu
        cursor.execute(f"""
            SELECT id, start_range, end_range, status, found, wif
            FROM {TABLE} 
            WHERE id >= ? AND status IN ('uncheck', '')
            ORDER BY id
            OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY
        """, (start_id, MAX_BATCHES_PER_RUN))
        
        batches = []
        columns = [column[0] for column in cursor.description]
        
        for row in cursor.fetchall():
            batch = dict(zip(columns, row))
            batches.append(batch)
        
        cursor.close()
        conn.close()
        
        return batches
        
    except Exception as e:
        print(f"❌ Error getting batch range: {e}")
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

def get_batch_summary():
    """Mendapatkan summary dari database"""
    conn = connect_db()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        
        # Hitung total dan status
        cursor.execute(f"""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END) as done,
                SUM(CASE WHEN status = 'inprogress' THEN 1 ELSE 0 END) as inprogress,
                SUM(CASE WHEN status = 'uncheck' THEN 1 ELSE 0 END) as uncheck,
                SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as error,
                SUM(CASE WHEN status = 'interrupted' THEN 1 ELSE 0 END) as interrupted,
                SUM(CASE WHEN found = 'Yes' THEN 1 ELSE 0 END) as found_yes,
                MIN(id) as min_id,
                MAX(id) as max_id
            FROM {TABLE}
        """)
        
        result = cursor.fetchone()
        columns = [column[0] for column in cursor.description]
        summary = dict(zip(columns, result))
        
        cursor.close()
        conn.close()
        
        return summary
        
    except Exception as e:
        print(f"❌ Error getting batch summary: {e}")
        if conn:
            conn.close()
        return None

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
            update_batch_status(batch_id, 'inprogress')
        
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
            update_batch_status(batch_id, 'interrupted')
        
        return 130, {'found': False}
    except Exception as e:
        error_msg = str(e)
        print(f"\n❌ Error: {error_msg}")
        
        # Update status error jika ada batch_id
        if batch_id is not None:
            update_batch_status(batch_id, 'error')
        
        return 1, {'found': False}

def display_database_summary():
    """Menampilkan summary dari database"""
    summary = get_batch_summary()
    
    if not summary:
        print("❌ Unable to get database summary")
        return
    
    print(f"\n{'='*50}")
    print("📊 DATABASE SUMMARY")
    print(f"{'='*50}")
    print(f"Total batches in DB: {summary['total']}")
    print(f"Batch ID range: {summary['min_id']} - {summary['max_id']}")
    print(f"\nStatus distribution:")
    print(f"  Done:        {summary['done'] or 0}")
    print(f"  In Progress: {summary['inprogress'] or 0}")
    print(f"  Unchecked:   {summary['uncheck'] or 0}")
    print(f"  Error:       {summary['error'] or 0}")
    print(f"  Interrupted: {summary['interrupted'] or 0}")
    print(f"\nFound private keys: {summary['found_yes'] or 0}")
    print(f"{'='*50}")

def main():
    global STOP_SEARCH_FLAG
    
    # Reset flag stop search setiap kali program dijalankan
    STOP_SEARCH_FLAG = False
    
    # Inisialisasi database
    if not initialize_database():
        print("❌ Failed to initialize database. Exiting...")
        sys.exit(1)
    
    # Parse arguments
    if len(sys.argv) < 2:
        print("Xiebo Batch Runner with SQL Server Database")
        print("Usage:")
        print("  Single run: python3 bm.py GPU_ID START_HEX RANGE_BITS ADDRESS")
        print("  Batch run from DB: python3 bm.py --batch-db GPU_ID START_ID ADDRESS")
        print("  Show summary: python3 bm.py --summary")
        print("\n⚠️  FEATURES:")
        print("  - Menggunakan database SQL Server")
        print("  - Baca range dari tabel Tbatch")
        print(f"  - Maksimal {MAX_BATCHES_PER_RUN} batch per eksekusi")
        print("  - Auto-stop ketika ditemukan Found: 1 atau lebih")
        sys.exit(1)
    
    # Show summary mode
    if sys.argv[1] == "--summary":
        display_database_summary()
        sys.exit(0)
    
    # Batch run from database mode
    elif sys.argv[1] == "--batch-db" and len(sys.argv) == 5:
        gpu_id = sys.argv[2]
        start_id = int(sys.argv[3])
        address = sys.argv[4]
        
        print(f"\n{'='*60}")
        print(f"BATCH MODE - DATABASE DRIVEN")
        print(f"{'='*60}")
        print(f"GPU: {gpu_id}")
        print(f"Start ID: {start_id}")
        print(f"Address: {address}")
        print(f"Max batches per run: {MAX_BATCHES_PER_RUN}")
        print(f"{'='*60}")
        
        # Ambil batch dari database
        batches = get_batch_range(start_id)
        
        if not batches or len(batches) == 0:
            print("❌ No batches found to process")
            sys.exit(1)
        
        print(f"\nFound {len(batches)} batches to process")
        
        # Jalankan setiap batch
        for i, batch in enumerate(batches):
            if STOP_SEARCH_FLAG:
                print(f"\n{'='*60}")
                print(f"🚨 AUTO-STOP TRIGGERED!")
                print(f"{'='*60}")
                print(f"Pencarian dihentikan karena private key telah ditemukan")
                print(f"Batch yang tersisa ({i+1}/{len(batches)}) tidak akan dijalankan")
                print(f"{'='*60}")
                break
            
            batch_id = batch['id']
            start_range = batch['start_range']
            end_range = batch['end_range']
            
            # Hitung range bits
            range_bits = calculate_range_bits(start_range, end_range)
            
            # Run this batch
            print(f"\n{'='*60}")
            print(f"▶️  BATCH {i+1}/{len(batches)} (ID: {batch_id})")
            print(f"{'='*60}")
            print(f"Start Range: {start_range}")
            print(f"End Range: {end_range}")
            print(f"Range Bits: {range_bits}")
            
            return_code, found_info = run_xiebo(gpu_id, start_range, range_bits, address, batch_id=batch_id)
            
            if return_code == 0:
                print(f"✅ Batch {batch_id} completed successfully")
            else:
                print(f"⚠️  Batch {batch_id} exited with code {return_code}")
            
            # Tampilkan progress
            if (i + 1) % 10 == 0 or i == len(batches) - 1:
                percentage = ((i + 1) / len(batches)) * 100
                print(f"\n📈 Progress: {i+1}/{len(batches)} batches ({percentage:.1f}%)")
            
            # Delay antara batch
            if i < len(batches) - 1 and not STOP_SEARCH_FLAG:
                print(f"\n⏱️  Waiting 5 seconds before next batch...")
                time.sleep(5)
        
        print(f"\n{'='*60}")
        if STOP_SEARCH_FLAG:
            print(f"🎯 SEARCH STOPPED - PRIVATE KEY FOUND!")
        else:
            print(f"✅ ALL BATCHES PROCESSED!")
        print(f"{'='*60}")
        
        # Tampilkan summary
        display_database_summary()
        
        # Cek jika ada private key yang ditemukan
        summary = get_batch_summary()
        if summary and summary['found_yes'] and summary['found_yes'] > 0:
            print(f"\n🔥 {summary['found_yes']} PRIVATE KEY(S) FOUND!")
            print(f"   Check database table Tbatch for details")
        
    # Single run mode (tetap support untuk backward compatibility)
    elif len(sys.argv) == 5:
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
    
    else:
        print("Invalid arguments")
        print("Usage: python3 bm.py GPU_ID START_HEX RANGE_BITS ADDRESS")
        print("Or:    python3 bm.py --batch-db GPU_ID START_ID ADDRESS")
        print("Or:    python3 bm.py --summary")
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
