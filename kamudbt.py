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

# Import untuk clear_output notebook
try:
    from IPython.display import clear_output, display, HTML
    IN_NOTEBOOK = True
    print("✅ Running in notebook environment - clear_output enabled")
    # Inisialisasi tracking untuk output notebook
    notebook_output_lines = 0
    MAX_NOTEBOOK_LINES = 100  # Maksimal baris sebelum clear
except ImportError:
    IN_NOTEBOOK = False
    print("⚠️  Running in terminal environment - clear_output disabled")

# Konfigurasi database SQL Server
SERVER = "benilapo-31088.portmap.host,31088"
DATABASE = "puzzleB53"
USERNAME = "sa"
PASSWORD = "LEtoy_89"
TABLE = "dbo.Tbatch"

# Global flag untuk menghentikan pencarian
STOP_SEARCH_FLAG = False

# Timer untuk clear output
LAST_CLEAR_TIME = time.time()
CLEAR_INTERVAL = 180  # 3 menit dalam detik

# Konfigurasi batch
MAX_BATCHES_PER_RUN = 6000000000000  # Maksimal 1juta batch per eksekusi
BATCH_SIZE = 2000000000000  # 2 triliun keys per batch

def clear_notebook_output():
    """Membersihkan output notebook dengan cara yang lebih efektif"""
    global LAST_CLEAR_TIME, notebook_output_lines
    
    if not IN_NOTEBOOK:
        return False
    
    current_time = time.time()
    
    # Clear berdasarkan interval waktu ATAU jika terlalu banyak baris
    if current_time - LAST_CLEAR_TIME >= CLEAR_INTERVAL or notebook_output_lines >= MAX_NOTEBOOK_LINES:
        try:
            clear_output(wait=True)
            print(f"🧹 Output cleared at {time.strftime('%H:%M:%S')} | Interval: {CLEAR_INTERVAL}s")
            LAST_CLEAR_TIME = current_time
            notebook_output_lines = 0
            return True
        except Exception as e:
            print(f"⚠️  Failed to clear output: {e}")
            return False
    
    return False

def print_notebook(text, end="\n"):
    """Fungsi print khusus untuk notebook yang melacak jumlah baris"""
    global notebook_output_lines
    
    if IN_NOTEBOOK:
        # Hitung jumlah baris baru dalam teks
        new_lines = text.count('\n') + (1 if end == '\n' else 0)
        notebook_output_lines += new_lines
        
        # Clear output jika terlalu banyak baris
        if notebook_output_lines >= MAX_NOTEBOOK_LINES:
            clear_notebook_output()
    
    # Gunakan print biasa
    print(text, end=end)

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
        print_notebook(f"❌ Database connection error: {e}")
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
        print_notebook(f"❌ Error getting batch by ID: {e}")
        if conn:
            conn.close()
        return None

def get_pending_batches(start_id, limit=100):
    """Mengambil batch yang pending mulai dari ID tertentu"""
    clear_notebook_output()  # Clear sebelum operasi database
    
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
        print_notebook(f"❌ Error getting pending batches: {e}")
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
        
        # Bersihkan output jika perlu sebelum mencetak status
        clear_notebook_output()
        print_notebook(f"📝 Updated batch {batch_id}: status={status}, found={found}")
        return True
        
    except Exception as e:
        print_notebook(f"❌ Error updating batch status: {e}")
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
        print_notebook(f"❌ Error calculating range bits: {e}")
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
                    clear_notebook_output()
                    print_notebook(f"🚨 STOP_SEARCH_FLAG diaktifkan karena Found: {found_count}")
        
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
    """Menampilkan output xiebo secara real-time dengan manajemen output notebook"""
    prefix = f"GPU {gpu_id}: " if gpu_id is not None else ""
    
    # Bersihkan output sebelum menampilkan header
    clear_notebook_output()
    
    print_notebook(f"\n{'─' * 80}")
    print_notebook(f"🎯 XIEBO OUTPUT (REAL-TIME){f' - GPU {gpu_id}' if gpu_id is not None else ''}:")
    print_notebook(f"{'─' * 80}")
    
    output_lines = []
    line_count = 0
    last_clear_check = time.time()
    batch_lines = []
    
    while True:
        output_line = process.stdout.readline()
        if output_line == '' and process.poll() is not None:
            break
        if output_line:
            # Cek apakah perlu membersihkan output
            if IN_NOTEBOOK:
                current_time = time.time()
                if current_time - last_clear_check >= 30:  # Cek setiap 30 detik
                    if clear_notebook_output():
                        last_clear_check = current_time
            
            # Tampilkan output dengan format yang lebih baik
            stripped_line = output_line.strip()
            if stripped_line:
                # Warna untuk output tertentu
                line_lower = stripped_line.lower()
                
                # Format output berdasarkan tipe pesan
                formatted_line = f"   {prefix}{stripped_line}"
                
                if 'found:' in line_lower or 'success' in line_lower:
                    # Line dengan hasil ditemukan
                    print_notebook(f"\033[92m{formatted_line}\033[0m")
                elif 'error' in line_lower or 'failed' in line_lower:
                    # Line dengan error
                    print_notebook(f"\033[91m{formatted_line}\033[0m")
                elif 'speed' in line_lower or 'key/s' in line_lower:
                    # Line dengan informasi speed
                    print_notebook(f"\033[93m{formatted_line}\033[0m")
                elif 'range' in line_lower:
                    # Line dengan informasi range
                    print_notebook(f"\033[94m{formatted_line}\033[0m")
                elif 'setting starting keys' in line_lower and '%' in line_lower:
                    # Filter progress "Setting starting keys" - tampilkan hanya setiap 5%
                    try:
                        # Ekstrak persentase
                        percent_match = re.search(r'\[(\d+\.?\d*)%\]', stripped_line)
                        if percent_match:
                            percent = float(percent_match.group(1))
                            if percent % 5 == 0 or percent >= 95:  # Tampilkan setiap 5% atau di atas 95%
                                print_notebook(formatted_line)
                    except:
                        print_notebook(formatted_line)
                else:
                    # Line normal
                    print_notebook(formatted_line)
                
                line_count += 1
                batch_lines.append(stripped_line)
                
                # Jika terlalu banyak baris dalam batch, clear
                if IN_NOTEBOOK and len(batch_lines) >= 50:
                    # Simpan progress terakhir
                    last_progress = ""
                    for line in reversed(batch_lines):
                        if 'key/s' in line.lower() or '%' in line.lower():
                            last_progress = line
                            break
                    
                    # Clear output
                    clear_notebook_output()
                    
                    # Tampilkan progress terakhir
                    if last_progress:
                        print_notebook(f"🧹 Continuing GPU {gpu_id} | Last progress: {last_progress}")
                    
                    batch_lines = []
            
            output_lines.append(output_line)
    
    output_text = ''.join(output_lines)
    
    # Bersihkan output sebelum menampilkan footer
    clear_notebook_output()
    print_notebook(f"{'─' * 80}")
    
    return output_text

def run_xiebo(gpu_id, start_hex, range_bits, address, batch_id=None):
    """Run xiebo binary untuk single GPU dengan batch tertentu"""
    global STOP_SEARCH_FLAG
    
    cmd = ["./xiebo", "-gpuId", str(gpu_id), "-start", start_hex, 
           "-range", str(range_bits), address]
    
    # Bersihkan output sebelum menampilkan header
    clear_notebook_output()
    
    print_notebook(f"\n{'='*80}")
    print_notebook(f"🚀 STARTING XIEBO EXECUTION - GPU {gpu_id}")
    print_notebook(f"{'='*80}")
    print_notebook(f"Command: {' '.join(cmd)}")
    print_notebook(f"Batch ID: {batch_id if batch_id is not None else 'N/A'}")
    print_notebook(f"{'='*80}")
    
    try:
        # Update status menjadi inprogress jika ada batch_id
        if batch_id is not None:
            update_batch_status(batch_id, 'inprogress')
        
        # Jalankan xiebo dan tampilkan output secara real-time
        print_notebook(f"\n⏳ Launching xiebo process for GPU {gpu_id}...")
        
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
        clear_notebook_output()
        print_notebook(f"\n{'='*80}")
        print_notebook(f"📊 SEARCH RESULT SUMMARY - GPU {gpu_id}")
        print_notebook(f"{'='*80}")
        
        if found_info['found_count'] > 0:
            print_notebook(f"\033[92m✅ FOUND: {found_info['found_count']} PRIVATE KEY(S)!\033[0m")
        elif found_info['found']:
            print_notebook(f"\033[92m✅ PRIVATE KEY FOUND!\033[0m")
        else:
            print_notebook(f"\033[93m❌ Private key not found in this batch\033[0m")
        
        if found_info['speed_info']:
            print_notebook(f"\n📈 Performance: {found_info['speed_info']}")
        
        if found_info['found'] or found_info['found_count'] > 0:
            print_notebook(f"\n📋 Found information:")
            if found_info['raw_output']:
                for line in found_info['raw_output'].split('\n'):
                    if 'found:' in line.lower() or 'priv' in line.lower():
                        print_notebook(f"\033[92m   {line}\033[0m")
                    else:
                        print_notebook(f"   {line}")
            else:
                if found_info['private_key_hex']:
                    print_notebook(f"   Priv (HEX): \033[92m{found_info['private_key_hex']}\033[0m")
                if found_info['private_key_wif']:
                    print_notebook(f"   Priv (WIF): \033[92m{found_info['private_key_wif']}\033[0m")
                if found_info['address']:
                    print_notebook(f"   Address: \033[92m{found_info['address']}\033[0m")
                if found_info['wif_key']:
                    print_notebook(f"   WIF Key (first 60 chars): \033[92m{found_info['wif_key']}\033[0m")
        
        print_notebook(f"{'='*80}")
        
        # Tampilkan return code
        if return_code == 0:
            print_notebook(f"\n🟢 Process completed successfully (return code: {return_code})")
        else:
            print_notebook(f"\n🟡 Process completed with return code: {return_code}")
        
        return return_code, found_info
        
    except KeyboardInterrupt:
        clear_notebook_output()
        print_notebook(f"\n\n{'='*80}")
        print_notebook(f"⚠️  STOPPED BY USER INTERRUPT (Ctrl+C) - GPU {gpu_id}")
        print_notebook(f"{'='*80}")
        
        # Update status jika batch diinterupsi
        if batch_id is not None:
            update_batch_status(batch_id, 'interrupted')
        
        return 130, {'found': False}
    except Exception as e:
        error_msg = str(e)
        clear_notebook_output()
        print_notebook(f"\n{'='*80}")
        print_notebook(f"❌ ERROR OCCURRED - GPU {gpu_id}")
        print_notebook(f"{'='*80}")
        print_notebook(f"Error: {error_msg}")
        print_notebook(f"{'='*80}")
        
        # Update status error jika ada batch_id
        if batch_id is not None:
            update_batch_status(batch_id, 'error')
        
        return 1, {'found': False}

# [Fungsi-fungsi lainnya tetap sama dengan penyesuaian print -> print_notebook]

def main():
    global STOP_SEARCH_FLAG, LAST_CLEAR_TIME
    
    # Reset flag stop search setiap kali program dijalankan
    STOP_SEARCH_FLAG = False
    LAST_CLEAR_TIME = time.time()
    
    # Parse arguments
    if len(sys.argv) < 2:
        clear_notebook_output()
        print_notebook("Xiebo Batch Runner with SQL Server Database & Multi-GPU Support")
        print_notebook(f"Notebook mode: {'ENABLED' if IN_NOTEBOOK else 'DISABLED'}")
        print_notebook(f"Clear output interval: {CLEAR_INTERVAL}s")
        print_notebook(f"Max lines before clear: {MAX_NOTEBOOK_LINES}")
        print_notebook("\nUsage:")
        print_notebook("  Single run: python3 bmdb.py GPU_ID START_HEX RANGE_BITS ADDRESS")
        print_notebook("  Batch parallel from DB: python3 bmdb.py --batch-db-parallel GPU_IDS START_ID ADDRESS")
        print_notebook("  Batch sequential from DB: python3 bmdb.py --batch-db-sequential GPU_IDS START_ID ADDRESS")
        print_notebook("\n⚠️  FEATURES:")
        print_notebook("  - Menggunakan database SQL Server")
        print_notebook("  - Baca range dari tabel Tbatch berdasarkan ID")
        print_notebook(f"  - Maksimal {MAX_BATCHES_PER_RUN} batch per eksekusi")
        print_notebook("  - Multi-GPU support (parallel dan sequential modes)")
        print_notebook("  - Auto-stop ketika ditemukan Found: 1 atau lebih")
        print_notebook("  - Real-time output display with colors")
        print_notebook("  - Auto-clear output every 3 minutes in notebook")
        print_notebook("  - Continue ke ID berikutnya secara otomatis")
        sys.exit(1)
    
    # [Bagian utama lainnya tetap sama dengan penyesuaian print -> print_notebook]

if __name__ == "__main__":
    # Check if xiebo exists
    if not os.path.exists("./xiebo"):
        print_notebook("❌ Error: xiebo binary not found in current directory")
        print_notebook("Please copy xiebo executable to this directory")
        sys.exit(1)
    
    # Check if executable
    if not os.access("./xiebo", os.X_OK):
        print_notebook("⚠️  xiebo is not executable, trying to fix...")
        os.chmod("./xiebo", 0o755)
    
    # Check for color support
    if os.name == 'posix':
        os.system('')  # Enable ANSI colors on Unix-like systems
    
    # Start main execution
    start_time = time.time()
    exit_code = main()
    end_time = time.time()
    
    # Display final summary
    if IN_NOTEBOOK:
        clear_notebook_output()
        print_notebook(f"\n{'='*80}")
        print_notebook(f"🏁 PROGRAM FINISHED")
        print_notebook(f"{'='*80}")
        print_notebook(f"Total runtime: {end_time - start_time:.2f} seconds")
        print_notebook(f"Notebook mode: ENABLED")
        print_notebook(f"Output cleared automatically every {CLEAR_INTERVAL} seconds or {MAX_NOTEBOOK_LINES} lines")
        print_notebook(f"{'='*80}")
    
    sys.exit(exit_code)
