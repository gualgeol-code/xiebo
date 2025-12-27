import sys
import os
import math
from datetime import datetime
import csv
import shutil
import threading
import queue
import time
import atexit
from concurrent.futures import ThreadPoolExecutor, as_completed

# Konfigurasi file log
LOG_FILE_PREFIX = "generated_batches"  # Prefix untuk file batch
LOG_FILE_EXT = ".txt"                  # Ekstensi file
NEXT_BATCH_FILE = "nextbatch.txt"      # File untuk menyimpan start range berikutnya
DRIVE_MOUNT_PATH = "/content/drive"
DRIVE_NEXT_BATCH_PATH = "/content/drive/MyDrive/nextbatch.txt"

# Kolom-kolom untuk tabel batch (hanya 2 kolom)
BATCH_COLUMNS = [
    'batch_id',
    'start_hex',
    'end_hex'
]

# Konfigurasi batch - sebagai variabel module-level
MAX_BATCHES_PER_RUN = 2000000          # Maksimal 1juta batch per eksekusi
BATCH_SIZE = 4000000000000            # 6 triliun keys per batch (default)
DEFAULT_ADDRESS = "N/A"                # Default address untuk batch generation
MAX_THREADS = 24                       # Jumlah thread maksimal untuk parallel processing

# Variabel global untuk tracking file batch
CURRENT_LOG_FILE = None                # File batch yang sedang aktif
LAST_UPLOADED_FILE = None              # File terakhir yang sudah diupload

# Global flag untuk kontrol thread
stop_monitor = threading.Event()

# Lock untuk thread safety
file_lock = threading.Lock()
print_lock = threading.Lock()

def safe_print(*args, **kwargs):
    """Thread-safe print function"""
    with print_lock:
        print(*args, **kwargs)

def cleanup_threads():
    """Cleanup function untuk menghentikan semua threads dengan aman"""
    global stop_monitor
    stop_monitor.set()
    time.sleep(0.1)  # Beri waktu untuk threads berhenti

# Register cleanup function
atexit.register(cleanup_threads)

def get_next_batch_filename():
    """Mendapatkan nama file batch berikutnya dengan penomoran"""
    index = 1
    while True:
        filename = f"{LOG_FILE_PREFIX}_{index:03d}{LOG_FILE_EXT}"
        if not os.path.exists(filename):
            return filename, index
        index += 1

def get_current_batch_file():
    """Mendapatkan file batch yang sedang aktif (file dengan index tertinggi)"""
    batch_files = []
    
    # Cari semua file batch
    for file in os.listdir('.'):
        if file.startswith(LOG_FILE_PREFIX) and file.endswith(LOG_FILE_EXT):
            batch_files.append(file)
    
    if not batch_files:
        # Jika tidak ada file batch, buat yang pertama
        filename, index = get_next_batch_filename()
        return filename
    
    # Urutkan berdasarkan index (generated_batches_001.txt, generated_batches_002.txt, dst)
    batch_files.sort()
    return batch_files[-1]  # File dengan index tertinggi

def get_current_batch_index():
    """Mendapatkan index file batch saat ini"""
    batch_files = []
    
    # Cari semua file batch
    for file in os.listdir('.'):
        if file.startswith(LOG_FILE_PREFIX) and file.endswith(LOG_FILE_EXT):
            batch_files.append(file)
    
    if not batch_files:
        return 1
    
    # Urutkan berdasarkan index
    batch_files.sort()
    
    # Ambil file terakhir
    last_file = batch_files[-1]
    
    # Ekstrak index dari nama file
    try:
        # Format: generated_batches_001.txt
        base_name = os.path.splitext(last_file)[0]  # generated_batches_001
        index_str = base_name.split('_')[-1]        # 001
        return int(index_str)
    except (ValueError, IndexError):
        return 1

def get_latest_batch_file():
    """Mendapatkan file batch terbaru (dengan index tertinggi)"""
    batch_files = []
    
    # Cari semua file batch
    for file in os.listdir('.'):
        if file.startswith(LOG_FILE_PREFIX) and file.endswith(LOG_FILE_EXT):
            batch_files.append(file)
    
    if not batch_files:
        return None
    
    # Urutkan berdasarkan index
    batch_files.sort()
    return batch_files[-1]  # File dengan index tertinggi

def should_create_new_batch_file(current_file, new_batch_count):
    """Menentukan apakah perlu membuat file batch baru"""
    if not os.path.exists(current_file):
        return True
    
    try:
        # Cek ukuran file saat ini
        file_size = os.path.getsize(current_file)
        
        # Jika file sudah besar (> 10MB) atau banyak batch (> 1000), buat file baru
        if file_size > 10 * 1024 * 1024:  # 10MB
            return True
        
        # Hitung jumlah batch dalam file saat ini
        batch_count = 0
        with open(current_file, 'r') as f:
            reader = csv.DictReader(f, delimiter='|')
            batch_count = sum(1 for _ in reader)
        
        if batch_count + new_batch_count > 10000:  # Jika akan melebihi 10,000 batch
            return True
            
    except Exception:
        pass
    
    return False

def save_to_drive(silent=False):
    """Menyimpan file ke Google Drive - HANYA file terakhir dan nextbatch.txt"""
    global LAST_UPLOADED_FILE
    
    try:
        # Cek apakah Google Drive tersedia (untuk Google Colab)
        if not os.path.exists(DRIVE_MOUNT_PATH):
            if not silent:
                safe_print("⚠️ Google Drive not mounted. Skipping save to drive.")
            return False
        
        try:
            from google.colab import drive
        except ImportError:
            if not silent:
                safe_print("⚠️ Google Colab not detected. Skipping Google Drive save.")
            return False
        
        # Mount drive jika belum
        drive_mydrive_path = os.path.join(DRIVE_MOUNT_PATH, "MyDrive")
        if not os.path.exists(drive_mydrive_path):
            if not silent:
                safe_print("🔄 Mounting Google Drive...")
            drive.mount(DRIVE_MOUNT_PATH, force_remount=False)
        
        uploaded_files = []
        
        # 1. Upload file batch terakhir saja
        latest_batch_file = get_latest_batch_file()
        if latest_batch_file:
            src = latest_batch_file
            dst = os.path.join(drive_mydrive_path, latest_batch_file)
            
            # Cek apakah file sudah diupload sebelumnya dan tidak berubah
            if LAST_UPLOADED_FILE == latest_batch_file and os.path.exists(dst):
                # Cek apakah file berubah
                src_mtime = os.path.getmtime(src) if os.path.exists(src) else 0
                dst_mtime = os.path.getmtime(dst) if os.path.exists(dst) else 0
                
                if src_mtime <= dst_mtime:
                    # File tidak berubah, skip upload
                    if not silent:
                        safe_print(f"  ⏭️  Skipping {latest_batch_file} (already uploaded and unchanged)")
                else:
                    # File berubah, upload ulang
                    shutil.copy2(src, dst)
                    LAST_UPLOADED_FILE = latest_batch_file
                    uploaded_files.append(latest_batch_file)
                    if not silent:
                        safe_print(f"  🔄 Updated {latest_batch_file} to Google Drive")
            else:
                # File baru atau belum diupload
                shutil.copy2(src, dst)
                LAST_UPLOADED_FILE = latest_batch_file
                uploaded_files.append(latest_batch_file)
                if not silent:
                    safe_print(f"  ✅ Saved {latest_batch_file} to Google Drive")
        
        # 2. Upload file nextbatch.txt jika ada
        if os.path.exists(NEXT_BATCH_FILE):
            src_next = NEXT_BATCH_FILE
            dst_next = os.path.join(drive_mydrive_path, "nextbatch.txt")
            
            # Cek apakah file nextbatch.txt berubah
            if os.path.exists(dst_next):
                src_mtime = os.path.getmtime(src_next) if os.path.exists(src_next) else 0
                dst_mtime = os.path.getmtime(dst_next) if os.path.exists(dst_next) else 0
                
                if src_mtime <= dst_mtime:
                    # File tidak berubah, skip
                    if not silent:
                        safe_print(f"  ⏭️  Skipping nextbatch.txt (already uploaded and unchanged)")
                else:
                    # File berubah, upload ulang
                    shutil.copy2(src_next, dst_next)
                    uploaded_files.append("nextbatch.txt")
                    if not silent:
                        safe_print(f"  🔄 Updated nextbatch.txt to Google Drive")
            else:
                # File baru
                shutil.copy2(src_next, dst_next)
                uploaded_files.append("nextbatch.txt")
                if not silent:
                    safe_print(f"  ✅ Saved nextbatch.txt to Google Drive")
        
        if not silent and uploaded_files:
            safe_print(f"📤 Uploaded {len(uploaded_files)} file(s) to Google Drive")
        
        return True
                
    except Exception as e:
        if not silent:
            safe_print(f"⚠️ Failed to save to Google Drive: {e}")
        return False

def read_all_batches_as_dict():
    """Membaca SEMUA file batch dan mengembalikan dictionary berdasarkan batch_id"""
    batch_dict = {}
    
    # Cari semua file batch
    batch_files = []
    for file in os.listdir('.'):
        if file.startswith(LOG_FILE_PREFIX) and file.endswith(LOG_FILE_EXT):
            batch_files.append(file)
    
    if not batch_files:
        return batch_dict
    
    # Urutkan file batch berdasarkan index
    batch_files.sort()
    
    # Baca semua file batch
    for batch_file in batch_files:
        try:
            with open(batch_file, 'r') as f:
                reader = csv.DictReader(f, delimiter='|')
                for row in reader:
                    batch_id = row.get('batch_id', '').strip()
                    if batch_id and batch_id not in batch_dict:
                        batch_dict[batch_id] = row
        except Exception as e:
            safe_print(f"⚠️ Error reading batch file {batch_file}: {e}")
    
    return batch_dict

def read_current_batches_as_dict():
    """Membaca file batch saat ini dan mengembalikan dictionary berdasarkan batch_id"""
    global CURRENT_LOG_FILE
    batch_dict = {}
    
    if CURRENT_LOG_FILE is None:
        CURRENT_LOG_FILE = get_current_batch_file()
    
    if not os.path.exists(CURRENT_LOG_FILE):
        return batch_dict
    
    try:
        with open(CURRENT_LOG_FILE, 'r') as f:
            reader = csv.DictReader(f, delimiter='|')
            for row in reader:
                batch_id = row.get('batch_id', '').strip()
                if batch_id:
                    batch_dict[batch_id] = row
    except Exception as e:
        safe_print(f"⚠️ Error reading batch file: {e}")
    
    return batch_dict

def verify_xiebo_compatibility_silent(start_hex, end_hex):
    """Verifikasi bahwa start dan end kompatibel dengan xiebo (silent mode)"""
    try:
        start_int = int(start_hex, 16)
        end_int = int(end_hex, 16)
        
        keys_count = end_int - start_int + 1
        
        # Xiebo hanya menerima range sebagai 2^N
        # Cek apakah keys_count adalah power of 2
        log2_val = math.log2(keys_count)
        
        return log2_val.is_integer()
            
    except Exception:
        return False

def verify_xiebo_compatibility(start_hex, end_hex, batch_id=None):
    """Verifikasi bahwa start dan end kompatibel dengan xiebo (verbose mode)"""
    try:
        start_int = int(start_hex, 16)
        end_int = int(end_hex, 16)
        
        keys_count = end_int - start_int + 1
        
        # Xiebo hanya menerima range sebagai 2^N
        # Cek apakah keys_count adalah power of 2
        log2_val = math.log2(keys_count)
        
        if batch_id:
            safe_print(f"\n🔍 XIEBO COMPATIBILITY CHECK - Batch {batch_id}:")
        else:
            safe_print(f"\n🔍 XIEBO COMPATIBILITY CHECK:")
            
        safe_print(f"   Start: 0x{start_hex} ({start_int:,})")
        safe_print(f"   End: 0x{end_hex} ({end_int:,})")
        safe_print(f"   Keys count: {keys_count:,}")
        safe_print(f"   Log2(keys_count): {log2_val:.6f}")
        
        if log2_val.is_integer():
            range_bits = int(log2_val)
            safe_print(f"   ✅ PERFECT: keys_count = 2^{range_bits} (compatible with xiebo)")
            safe_print(f"   Xiebo will use: -range {range_bits}")
            safe_print(f"   Expected xiebo end: 0x{format(start_int + (1 << range_bits) - 1, 'x')}")
            return True, range_bits
        else:
            required_bits = int(math.ceil(log2_val))
            expected_end = start_int + (1 << required_bits) - 1
            extra_keys = (1 << required_bits) - keys_count
            
            safe_print(f"   ⚠️  WARNING: keys_count is NOT power of 2")
            safe_print(f"   Xiebo will use: -range {required_bits}")
            safe_print(f"   Xiebo will search: {1 << required_bits:,} keys")
            safe_print(f"   Xiebo expected end: 0x{format(expected_end, 'x')}")
            safe_print(f"   Database end: 0x{end_hex}")
            safe_print(f"   Extra keys searched: {extra_keys:,}")
            safe_print(f"   Difference: {expected_end - end_int:,}")
            
            # If the difference is small, it's acceptable
            if extra_keys <= 1000:
                safe_print(f"   ✅ ACCEPTABLE: Small difference ({extra_keys:,} extra keys)")
                return True, required_bits
            else:
                safe_print(f"   ❌ PROBLEM: Large difference ({extra_keys:,} extra keys)")
                return False, required_bits
            
    except Exception as e:
        safe_print(f"❌ Error verifying xiebo compatibility: {e}")
        return False, 0

def write_batches_from_dict(batch_dict, create_new_file=False):
    """Menulis batch file dari dictionary - DENGAN VERIFIKASI XIEBO"""
    global CURRENT_LOG_FILE
    
    try:
        # Tentukan file batch yang akan digunakan
        if create_new_file or CURRENT_LOG_FILE is None:
            # Cari file batch berikutnya
            filename, index = get_next_batch_filename()
            CURRENT_LOG_FILE = filename
            safe_print(f"📁 Creating new batch file: {CURRENT_LOG_FILE}")
        elif CURRENT_LOG_FILE is None:
            CURRENT_LOG_FILE = get_current_batch_file()
        
        # Konversi dictionary ke list dan urutkan
        rows = []
        for batch_id in sorted(batch_dict.keys(), key=lambda x: int(x) if x.isdigit() else x):
            rows.append(batch_dict[batch_id])
        
        # Verifikasi xiebo compatibility sebelum menulis
        safe_print(f"\n🔍 VERIFYING XIEBO COMPATIBILITY FOR ALL BATCHES:")
        
        verified_count = 0
        problematic_count = 0
        problematic_batches = []
        
        for batch in rows:
            batch_id = batch['batch_id']
            is_compatible, _ = verify_xiebo_compatibility_silent(batch['start_hex'], batch['end_hex'])
            
            if is_compatible:
                verified_count += 1
            else:
                problematic_count += 1
                problematic_batches.append(batch_id)
        
        # Tulis ke file dengan format tabel
        with open(CURRENT_LOG_FILE, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=BATCH_COLUMNS, delimiter='|')
            writer.writeheader()
            writer.writerows(rows)
        
        safe_print(f"\n💾 Batch data saved to: {CURRENT_LOG_FILE}")
        safe_print(f"📊 Total batches in file: {len(rows)}")
        safe_print(f"✅ Xiebo-compatible batches: {verified_count}")
        
        if problematic_count > 0:
            safe_print(f"⚠️  Problematic batches (not power of 2): {problematic_count}")
            safe_print(f"   Batch IDs: {', '.join(problematic_batches[:10])}" + 
                      ("..." if len(problematic_batches) > 10 else ""))
            safe_print(f"   These batches may cause range mismatch with xiebo!")
        
        # Simpan ke Google Drive (dengan feedback) - HANYA upload file ini
        safe_print(f"\n🔄 Saving to Google Drive...")
        save_to_drive(silent=False)
        
    except Exception as e:
        safe_print(f"❌ Error writing batch file: {e}")

def save_next_batch_info(start_hex, range_bits, address, next_start_hex, batches_generated, total_batches, timestamp=None):
    """Menyimpan informasi batch berikutnya ke file"""
    global CURRENT_LOG_FILE
    
    try:
        if timestamp is None:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        info = {
            'original_start': start_hex,
            'original_range_bits': str(range_bits),
            'address': address,
            'next_start_hex': next_start_hex,
            'batches_generated': str(batches_generated),
            'total_batches': str(total_batches),
            'timestamp': timestamp,
            'current_batch_file': CURRENT_LOG_FILE if CURRENT_LOG_FILE else get_current_batch_file(),
            'current_batch_index': str(get_current_batch_index())
        }
        
        # 1. Simpan ke file nextbatch.txt
        with open(NEXT_BATCH_FILE, 'w') as f:
            for key, value in info.items():
                f.write(f"{key}={value}\n")
        
        # 2. Simpan ke Google Drive - HANYA upload file nextbatch.txt
        safe_print(f"\n🔄 Saving next batch info to Google Drive...")
        save_to_drive(silent=False)
        
        safe_print(f"\n📝 Next batch info saved:")
        safe_print(f"   File: {NEXT_BATCH_FILE}")
        safe_print(f"   Next start: 0x{next_start_hex}")
        safe_print(f"   Progress: {batches_generated}/{total_batches} batches generated")
        safe_print(f"   Current batch file: {info['current_batch_file']}")
        safe_print(f"   Current batch index: {info['current_batch_index']}")
        
    except Exception as e:
        safe_print(f"❌ Error saving next batch info: {e}")

def load_next_batch_info():
    """Memuat informasi batch berikutnya dari file"""
    global CURRENT_LOG_FILE
    
    if not os.path.exists(NEXT_BATCH_FILE):
        return None
    
    try:
        info = {}
        with open(NEXT_BATCH_FILE, 'r') as f:
            for line in f:
                line = line.strip()
                if '=' in line:
                    key, value = line.split('=', 1)
                    info[key] = value
        
        # Set current batch file
        if 'current_batch_file' in info:
            CURRENT_LOG_FILE = info['current_batch_file']
        
        return info
    except Exception as e:
        safe_print(f"❌ Error loading next batch info from file: {e}")
        return None

def calculate_range_bits(keys_count):
    """Menghitung range bits yang benar untuk jumlah keys tertentu - SAMA DENGAN XIEBO"""
    if keys_count <= 1:
        return 1
    
    # Xiebo menggunakan: range = 2^N
    # Jadi kita perlu mencari N terkecil sehingga 2^N >= keys_count
    log2_val = math.log2(keys_count)
    
    # SELALU gunakan CEIL untuk memastikan semua keys tercakup
    # Ini sesuai dengan cara xiebo bekerja
    return int(math.ceil(log2_val))

def adjust_batch_size_for_xiebo(batch_size):
    """Sesuaikan batch size agar menjadi power of 2 (kompatibel dengan xiebo)"""
    # Hitung range bits untuk batch size yang diminta
    batch_range_bits = calculate_range_bits(batch_size)
    
    # Hitung batch size yang sesuai dengan 2^N
    adjusted_size = 1 << batch_range_bits
    
    return adjusted_size, batch_range_bits

def generate_batch_worker(args):
    """Worker function untuk generate batch dalam thread - SESUAI XIEBO"""
    start_int, adjusted_batch_size, batch_range_bits, end_int, start_batch_id, i = args
    batch_id = start_batch_id + i
    batch_start = start_int + (i * adjusted_batch_size)
    
    # Hitung batch_end berdasarkan batch size yang sudah disesuaikan
    batch_end = min(batch_start + adjusted_batch_size, end_int + 1)
    batch_keys = batch_end - batch_start
    
    # Pastikan end sesuai dengan xiebo: end = start + 2^N - 1
    # Tapi juga pastikan tidak melebihi total end_int
    if batch_end - 1 <= end_int:
        batch_end_hex = format(batch_end - 1, 'x')  # end inklusif
    else:
        batch_end_hex = format(end_int, 'x')  # tidak melebihi total range
    
    batch_start_hex = format(batch_start, 'x')
    
    # Verifikasi bahwa range ini power of 2 (kecuali batch terakhir yang mungkin lebih kecil)
    actual_keys = int(batch_end_hex, 16) - int(batch_start_hex, 16) + 1
    
    # Buat informasi batch (hanya 2 kolom)
    batch_info = {
        'batch_id': str(batch_id),
        'start_hex': batch_start_hex,
        'end_hex': batch_end_hex
    }
    
    return batch_id, batch_info, batch_keys, i, actual_keys

def generate_batches_multithreaded(start_hex, range_bits, address, batch_size, start_batch_id=0, max_batches=None):
    """Generate batch dari range hex menggunakan multithreading - SESUAI XIEBO"""
    global CURRENT_LOG_FILE, stop_monitor
    
    start_int = int(start_hex, 16)
    total_keys = 1 << range_bits  # Xiebo: total = 2^range_bits
    end_int = start_int + total_keys - 1  # Xiebo: end = start + 2^N - 1
    
    # SESUAIKAN BATCH SIZE AGAR POWER OF 2 (kompatibel dengan xiebo)
    adjusted_batch_size, batch_range_bits = adjust_batch_size_for_xiebo(batch_size)
    
    safe_print(f"\n{'='*60}")
    safe_print(f"GENERATING BATCHES - XIEBO COMPATIBLE MODE")
    safe_print(f"{'='*60}")
    safe_print(f"🚀 MAIN RANGE CONFIGURATION:")
    safe_print(f"   Start: 0x{start_hex}")
    safe_print(f"   Range bits: {range_bits} bits (Xiebo format)")
    safe_print(f"   Total keys: {total_keys:,} (2^{range_bits})")
    safe_print(f"   End: 0x{format(end_int, 'x')} (start + 2^{range_bits} - 1)")
    safe_print(f"\n📏 BATCH SIZE CONFIGURATION:")
    safe_print(f"   Requested batch size: {batch_size:,}")
    safe_print(f"   Calculated batch range bits: {batch_range_bits}")
    safe_print(f"   Adjusted batch size: {adjusted_batch_size:,} (2^{batch_range_bits})")
    safe_print(f"   Difference: {adjusted_batch_size - batch_size:,} keys")
    
    total_batches_needed = math.ceil(total_keys / adjusted_batch_size)
    
    # Limit jumlah batch jika ada max_batches
    if max_batches is not None:
        batches_to_generate = min(total_batches_needed, max_batches)
    else:
        batches_to_generate = total_batches_needed
    
    safe_print(f"\n📊 BATCH GENERATION PLAN:")
    safe_print(f"   Address: {address}")
    safe_print(f"   Total batches needed: {total_batches_needed:,}")
    safe_print(f"   Batches to generate this run: {batches_to_generate}")
    safe_print(f"   Starting batch ID: {start_batch_id}")
    safe_print(f"   Output format: {BATCH_COLUMNS}")
    safe_print(f"   Threads: {MAX_THREADS}")
    safe_print(f"{'='*60}")
    
    # Tentukan apakah perlu membuat file baru
    create_new_file = False
    if start_batch_id == 0:
        # Jika mulai dari awal, buat file baru
        create_new_file = True
    elif CURRENT_LOG_FILE and os.path.exists(CURRENT_LOG_FILE):
        # Cek apakah file saat ini sudah besar
        create_new_file = should_create_new_batch_file(CURRENT_LOG_FILE, batches_to_generate)
    
    if create_new_file:
        safe_print(f"🆕 Creating new batch file for this run...")
    
    # Baca batch yang sudah ada (jika melanjutkan dan tidak membuat file baru)
    if start_batch_id > 0 and not create_new_file:
        batch_dict = read_current_batches_as_dict()
        # Juga baca dari file sebelumnya jika ada
        existing_batches = read_all_batches_as_dict()
        # Gabungkan, prioritaskan yang baru
        batch_dict.update(existing_batches)
    else:
        batch_dict = {}
    
    # Progress tracking
    progress_queue = queue.Queue()
    completed_count = 0
    start_time = time.time()
    
    def progress_monitor(total):
        """Monitor progress dari queue"""
        nonlocal completed_count
        last_update = 0
        update_interval = 0.5  # Update setiap 0.5 detik
        
        while completed_count < total and not stop_monitor.is_set():
            try:
                current_time = time.time()
                if current_time - last_update >= update_interval:
                    # Cek jika ada progress update
                    while not progress_queue.empty():
                        try:
                            batch_id, batch_idx, batch_keys, actual_keys = progress_queue.get_nowait()
                            completed_count += 1
                            
                            # Tampilkan info batch pertama dan setiap 100 batch
                            if completed_count == 1 or completed_count % 100 == 0 or completed_count == total:
                                # Verifikasi xiebo compatibility untuk batch ini
                                with file_lock:
                                    if str(batch_id) in batch_dict:
                                        batch_data = batch_dict[str(batch_id)]
                                        is_power_of_2 = verify_xiebo_compatibility_silent(
                                            batch_data['start_hex'], 
                                            batch_data['end_hex']
                                        )
                                        status = "✅" if is_power_of_2 else "⚠️"
                                        safe_print(f"   {status} Batch {batch_id}: {batch_keys:,} keys")
                            
                        except queue.Empty:
                            break
                    
                    # Display progress
                    if completed_count > 0:
                        elapsed = current_time - start_time
                        batches_per_sec = completed_count / elapsed if elapsed > 0 else 0
                        remaining = total - completed_count
                        eta = remaining / batches_per_sec if batches_per_sec > 0 else 0
                        
                        safe_print(f"✅ Progress: {completed_count}/{total} batches "
                                  f"({completed_count/total*100:.1f}%), "
                                  f"Speed: {batches_per_sec:.1f} batches/sec, "
                                  f"ETA: {eta:.0f}s", end='\r')
                    
                    last_update = current_time
                
                time.sleep(0.1)
                
            except Exception as e:
                safe_print(f"\n⚠️ Progress monitor error: {e}")
                break
        
        # Final update
        if completed_count > 0:
            safe_print(f"\n✅ Completed {completed_count}/{total} batches "
                      f"({completed_count/total*100:.1f}%)")
    
    # Reset stop_monitor flag
    stop_monitor.clear()
    
    # Mulai progress monitor thread (NON-DAEMON)
    monitor_thread = threading.Thread(target=progress_monitor, args=(batches_to_generate,))
    monitor_thread.start()
    
    # Statistik
    power_of_2_count = 0
    non_power_of_2_count = 0
    
    try:
        # Gunakan ThreadPoolExecutor untuk parallel processing
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            # Prepare arguments untuk semua batch
            batch_args = [(start_int, adjusted_batch_size, batch_range_bits, end_int, start_batch_id, i) 
                         for i in range(batches_to_generate)]
            
            # Submit semua tasks
            futures = [executor.submit(generate_batch_worker, arg) for arg in batch_args]
            
            # Process hasil
            for future in as_completed(futures):
                try:
                    batch_id, batch_info, batch_keys, batch_idx, actual_keys = future.result()
                    
                    # Cek apakah batch ini power of 2
                    is_power_of_2 = verify_xiebo_compatibility_silent(
                        batch_info['start_hex'], 
                        batch_info['end_hex']
                    )
                    
                    if is_power_of_2:
                        power_of_2_count += 1
                    else:
                        non_power_of_2_count += 1
                        # Batch terakhir mungkin bukan power of 2, itu normal
                        if batch_idx == batches_to_generate - 1:
                            safe_print(f"   ℹ️  Last batch {batch_id}: {actual_keys:,} keys (may not be power of 2)")
                    
                    # Simpan hasil ke dictionary dengan lock untuk thread safety
                    with file_lock:
                        batch_dict[str(batch_id)] = batch_info
                    
                    # Kirim progress update ke queue
                    progress_queue.put((batch_id, batch_idx, batch_keys, actual_keys))
                    
                except Exception as e:
                    safe_print(f"\n❌ Error generating batch: {e}")
        
        # Tunggu progress monitor selesai
        monitor_thread.join(timeout=5)
        
        # Set stop flag untuk monitor thread
        stop_monitor.set()
        
    except KeyboardInterrupt:
        safe_print("\n\n⚠️ Generation interrupted by user")
        stop_monitor.set()
        monitor_thread.join(timeout=2)
        raise
    
    except Exception as e:
        safe_print(f"\n❌ Error in multithreaded generation: {e}")
        stop_monitor.set()
        monitor_thread.join(timeout=2)
        raise
    
    finally:
        # Pastikan monitor thread berhenti
        if monitor_thread.is_alive():
            stop_monitor.set()
            monitor_thread.join(timeout=2)
    
    # Hitung statistik akhir
    elapsed_time = time.time() - start_time
    batches_per_second = batches_to_generate / elapsed_time if elapsed_time > 0 else 0
    
    safe_print(f"\n{'='*60}")
    safe_print(f"📊 GENERATION STATISTICS:")
    safe_print(f"   Total time: {elapsed_time:.2f} seconds")
    safe_print(f"   Batches per second: {batches_per_second:.2f}")
    safe_print(f"   Threads used: {MAX_THREADS}")
    safe_print(f"   Power of 2 batches: {power_of_2_count}")
    safe_print(f"   Non-power of 2 batches: {non_power_of_2_count}")
    safe_print(f"   Adjusted batch size: {adjusted_batch_size:,} (2^{batch_range_bits})")
    safe_print(f"{'='*60}")
    
    # Tulis batch ke file
    write_batches_from_dict(batch_dict, create_new_file)
    
    # Simpan info batch berikutnya jika belum selesai semua
    if batches_to_generate < total_batches_needed:
        next_start_int = start_int + (batches_to_generate * adjusted_batch_size)
        next_start_hex = format(next_start_int, 'x')
        
        save_next_batch_info(
            start_hex,
            range_bits,
            address,
            next_start_hex,
            start_batch_id + batches_to_generate,
            total_batches_needed
        )
    else:
        # Jika semua batch selesai, hapus file nextbatch.txt
        if os.path.exists(NEXT_BATCH_FILE):
            os.remove(NEXT_BATCH_FILE)
            safe_print(f"\n🗑️  All batches completed. Removed {NEXT_BATCH_FILE}")
        
        # Upload final file ke Google Drive
        safe_print(f"\n🔄 Uploading final batch file to Google Drive...")
        save_to_drive(silent=False)
    
    return total_batches_needed, batches_to_generate, batch_dict

def generate_batches_single_thread(start_hex, range_bits, address, batch_size, start_batch_id=0, max_batches=None):
    """Generate batch dari range hex (single thread - legacy)"""
    global CURRENT_LOG_FILE
    
    start_int = int(start_hex, 16)
    total_keys = 1 << range_bits  # Xiebo: total = 2^range_bits
    end_int = start_int + total_keys - 1  # Xiebo: end = start + 2^N - 1
    
    # SESUAIKAN BATCH SIZE AGAR POWER OF 2
    adjusted_batch_size, batch_range_bits = adjust_batch_size_for_xiebo(batch_size)
    
    total_batches_needed = math.ceil(total_keys / adjusted_batch_size)
    
    # Limit jumlah batch jika ada max_batches
    if max_batches is not None:
        batches_to_generate = min(total_batches_needed, max_batches)
    else:
        batches_to_generate = total_batches_needed
    
    safe_print(f"\n{'='*60}")
    safe_print(f"GENERATING BATCHES - SINGLE THREAD (Xiebo Compatible)")
    safe_print(f"{'='*60}")
    safe_print(f"   Original batch size: {batch_size:,}")
    safe_print(f"   Adjusted batch size: {adjusted_batch_size:,} (2^{batch_range_bits})")
    
    # Tentukan apakah perlu membuat file baru
    create_new_file = False
    if start_batch_id == 0:
        # Jika mulai dari awal, buat file baru
        create_new_file = True
    elif CURRENT_LOG_FILE and os.path.exists(CURRENT_LOG_FILE):
        # Cek apakah file saat ini sudah besar
        create_new_file = should_create_new_batch_file(CURRENT_LOG_FILE, batches_to_generate)
    
    if create_new_file:
        safe_print(f"🆕 Creating new batch file for this run...")
    
    # Baca batch yang sudah ada (jika melanjutkan dan tidak membuat file baru)
    if start_batch_id > 0 and not create_new_file:
        batch_dict = read_current_batches_as_dict()
        # Juga baca dari file sebelumnya jika ada
        existing_batches = read_all_batches_as_dict()
        # Gabungkan, prioritaskan yang baru
        batch_dict.update(existing_batches)
    else:
        batch_dict = {}
    
    start_time = time.time()
    
    for i in range(batches_to_generate):
        batch_id = start_batch_id + i
        batch_start = start_int + (i * adjusted_batch_size)
        batch_end = min(batch_start + adjusted_batch_size, end_int + 1)
        batch_keys = batch_end - batch_start
        
        # Pastikan end sesuai dengan xiebo: end = start + 2^N - 1
        if batch_end - 1 <= end_int:
            batch_end_hex = format(batch_end - 1, 'x')  # end inklusif
        else:
            batch_end_hex = format(end_int, 'x')  # tidak melebihi total range
        
        batch_start_hex = format(batch_start, 'x')
        
        # Verifikasi xiebo compatibility
        is_power_of_2 = verify_xiebo_compatibility_silent(batch_start_hex, batch_end_hex)
        
        # Buat informasi batch (hanya 2 kolom)
        batch_info = {
            'batch_id': str(batch_id),
            'start_hex': batch_start_hex,
            'end_hex': batch_end_hex
        }
        
        batch_dict[str(batch_id)] = batch_info
        
        # Tampilkan progress setiap 10 batch atau batch terakhir
        if (i + 1) % 10 == 0 or i == batches_to_generate - 1:
            elapsed = time.time() - start_time
            batches_per_sec = (i + 1) / elapsed if elapsed > 0 else 0
            
            status = "✅" if is_power_of_2 else "⚠️"
            safe_print(f"{status} Generated batch {i+1}/{batches_to_generate}: ID={batch_id}, "
                      f"Keys={batch_keys:,}, Speed={batches_per_sec:.1f} batches/sec")
    
    elapsed_time = time.time() - start_time
    safe_print(f"\n⏱️  Generation time: {elapsed_time:.2f} seconds")
    
    # Tulis batch ke file
    write_batches_from_dict(batch_dict, create_new_file)
    
    # Simpan info batch berikutnya jika belum selesai semua
    if batches_to_generate < total_batches_needed:
        next_start_int = start_int + (batches_to_generate * adjusted_batch_size)
        next_start_hex = format(next_start_int, 'x')
        
        save_next_batch_info(
            start_hex,
            range_bits,
            address,
            next_start_hex,
            start_batch_id + batches_to_generate,
            total_batches_needed
        )
    
    return total_batches_needed, batches_to_generate, batch_dict

def continue_generation_auto(batch_size, max_batches=None):
    """Lanjutkan generate batch dari state yang tersimpan secara otomatis sampai selesai TANPA KONFIRMASI"""
    global CURRENT_LOG_FILE, stop_monitor
    
    run_count = 0
    
    try:
        while True:
            next_info = load_next_batch_info()
            if not next_info:
                safe_print("✅ All batches have been generated!")
                break
            
            run_count += 1
            safe_print(f"\n{'='*60}")
            safe_print(f"CONTINUE GENERATION (AUTO) - RUN #{run_count}")
            safe_print(f"{'='*60}")
            
            start_hex = next_info['next_start_hex']
            range_bits = int(next_info['original_range_bits'])
            address = next_info['address']
            batches_generated = int(next_info['batches_generated'])
            total_batches = int(next_info['total_batches'])
            current_file = next_info.get('current_batch_file', CURRENT_LOG_FILE)
            
            # Set current file
            CURRENT_LOG_FILE = current_file
            
            safe_print(f"Resuming from saved state...")
            safe_print(f"Next start: 0x{start_hex}")
            safe_print(f"Range: {range_bits} bits")
            safe_print(f"Address: {address}")
            safe_print(f"Batches already generated: {batches_generated:,}")
            safe_print(f"Total batches needed: {total_batches:,}")
            safe_print(f"Current batch file: {current_file}")
            safe_print(f"Timestamp: {next_info.get('timestamp', 'unknown')}")
            safe_print(f"Output format: {BATCH_COLUMNS}")
            safe_print(f"Threads: {MAX_THREADS}")
            safe_print(f"{'='*60}")
            
            # Hitung jumlah batch yang tersisa
            remaining_batches = total_batches - batches_generated
            
            if remaining_batches <= 0:
                safe_print("✅ All batches already generated!")
                break
            
            # Limit jumlah batch jika ada max_batches
            if max_batches is not None:
                batches_to_generate = min(remaining_batches, max_batches)
            else:
                batches_to_generate = remaining_batches
            
            safe_print(f"\nGenerating {batches_to_generate:,} more batches")
            safe_print(f"{remaining_batches:,} batches remaining in total")
            
            # Generate batch menggunakan multithreading
            total_batches_needed, actual_generated, batch_dict = generate_batches_multithreaded(
                start_hex, range_bits, address, batch_size, 
                start_batch_id=batches_generated, max_batches=batches_to_generate
            )
            
            safe_print(f"\n{'='*60}")
            safe_print(f"✅ GENERATION COMPLETED FOR RUN #{run_count}")
            safe_print(f"{'='*60}")
            safe_print(f"Generated {actual_generated:,} new batches")
            safe_print(f"Total batches generated so far: {batches_generated + actual_generated:,}/{total_batches:,}")
            safe_print(f"Progress: {((batches_generated + actual_generated) / total_batches * 100):.2f}%")
            
            # Cek apakah masih ada batch yang tersisa
            next_info_after = load_next_batch_info()
            if not next_info_after:
                safe_print(f"\n{'='*60}")
                safe_print(f"🎉 ALL BATCHES COMPLETED!")
                safe_print(f"{'='*60}")
                safe_print(f"Total runs: {run_count}")
                safe_print(f"Total batches: {batches_generated + actual_generated:,}")
                
                # Upload final summary
                safe_print(f"\n🔄 Uploading final summary to Google Drive...")
                save_to_drive(silent=False)
                break
            
            if batches_generated + actual_generated >= total_batches:
                safe_print(f"\n{'='*60}")
                safe_print(f"🎉 ALL BATCHES COMPLETED!")
                safe_print(f"{'='*60}")
                safe_print(f"Total runs: {run_count}")
                safe_print(f"Total batches: {batches_generated + actual_generated:,}")
                
                # Upload final summary
                safe_print(f"\n🔄 Uploading final summary to Google Drive...")
                save_to_drive(silent=False)
                break
            
            # LANGSUNG LANJUT TANPA KONFIRMASI
            safe_print(f"\n{'='*60}")
            safe_print(f"⏭️  Auto-continuing to next run...")
            safe_print(f"Progress: {batches_generated + actual_generated:,}/{total_batches:,} batches ({((batches_generated + actual_generated) / total_batches * 100):.2f}%)")
            safe_print(f"Current batch file: {CURRENT_LOG_FILE}")
            safe_print(f"{'='*60}")
            
            # Beri jeda kecil sebelum melanjutkan
            time.sleep(0.5)
    
    except KeyboardInterrupt:
        safe_print("\n\n⚠️ Auto-continue interrupted by user")
        stop_monitor.set()
    
    except Exception as e:
        safe_print(f"\n❌ Error in auto-continue: {e}")
        stop_monitor.set()
        raise

def continue_generation_single(batch_size, max_batches=None, use_multithread=True):
    """Lanjutkan generate batch dari state yang tersimpan (single run)"""
    global CURRENT_LOG_FILE
    
    next_info = load_next_batch_info()
    if not next_info:
        safe_print("❌ No saved state found. Run with --generate first.")
        sys.exit(1)
    
    start_hex = next_info['next_start_hex']
    range_bits = int(next_info['original_range_bits'])
    address = next_info['address']
    batches_generated = int(next_info['batches_generated'])
    total_batches = int(next_info['total_batches'])
    current_file = next_info.get('current_batch_file', CURRENT_LOG_FILE)
    
    # Set current file
    CURRENT_LOG_FILE = current_file
    
    safe_print(f"\n{'='*60}")
    safe_print(f"CONTINUE GENERATION (SINGLE RUN)")
    safe_print(f"{'='*60}")
    safe_print(f"Current batch file: {CURRENT_LOG_FILE}")
    
    remaining_batches = total_batches - batches_generated
    batches_to_generate = min(remaining_batches, max_batches) if max_batches else remaining_batches
    
    if batches_to_generate <= 0:
        safe_print("✅ All batches already generated!")
        sys.exit(0)
    
    # Pilih metode berdasarkan parameter
    if use_multithread:
        total_batches_needed, actual_generated, batch_dict = generate_batches_multithreaded(
            start_hex, range_bits, address, batch_size, 
            start_batch_id=batches_generated, max_batches=batches_to_generate
        )
    else:
        total_batches_needed, actual_generated, batch_dict = generate_batches_single_thread(
            start_hex, range_bits, address, batch_size, 
            start_batch_id=batches_generated, max_batches=batches_to_generate
        )
    
    safe_print(f"\n{'='*60}")
    safe_print(f"✅ GENERATION COMPLETED")
    safe_print(f"{'='*60}")
    safe_print(f"Generated {actual_generated} new batches")
    safe_print(f"Total batches generated so far: {batches_generated + actual_generated}/{total_batches}")
    safe_print(f"Current batch file: {CURRENT_LOG_FILE}")
    
    display_batch_summary()

def display_batch_summary():
    """Menampilkan summary batch yang telah digenerate"""
    batch_dict = read_all_batches_as_dict()
    
    if len(batch_dict) == 0:
        safe_print("📭 No batch data found")
        return
    
    try:
        total_batches = len(batch_dict)
        
        # Hitung total file batch
        batch_files = []
        for file in os.listdir('.'):
            if file.startswith(LOG_FILE_PREFIX) and file.endswith(LOG_FILE_EXT):
                batch_files.append(file)
        
        safe_print(f"\n{'='*60}")
        safe_print(f"📊 BATCH SUMMARY (Xiebo Compatible)")
        safe_print(f"{'='*60}")
        safe_print(f"Total batches generated: {total_batches}")
        safe_print(f"Total batch files: {len(batch_files)}")
        
        # Hitung berapa banyak batch yang kompatibel dengan xiebo
        xiebo_compatible = 0
        non_xiebo_compatible = 0
        
        for batch_id, batch in batch_dict.items():
            if verify_xiebo_compatibility_silent(batch['start_hex'], batch['end_hex']):
                xiebo_compatible += 1
            else:
                non_xiebo_compatible += 1
        
        safe_print(f"\n🔍 XIEBO COMPATIBILITY ANALYSIS:")
        safe_print(f"   Compatible batches (power of 2): {xiebo_compatible}")
        safe_print(f"   Non-compatible batches: {non_xiebo_compatible}")
        safe_print(f"   Compatibility rate: {xiebo_compatible/total_batches*100:.1f}%")
        
        if non_xiebo_compatible > 0:
            safe_print(f"   ⚠️  Warning: {non_xiebo_compatible} batches may cause range mismatch!")
        
        # Tampilkan daftar file batch dengan detail
        if batch_files:
            batch_files.sort()
            safe_print(f"\n📁 Batch files:")
            total_file_size = 0
            total_file_batches = 0
            
            for file in batch_files:
                file_size = os.path.getsize(file)
                total_file_size += file_size
                
                # Hitung batch dalam file
                file_batches = 0
                try:
                    with open(file, 'r') as f:
                        reader = csv.DictReader(f, delimiter='|')
                        file_batches = sum(1 for _ in reader)
                    total_file_batches += file_batches
                except:
                    pass
                
                marker = " 🟢" if file == get_latest_batch_file() else ""
                safe_print(f"  {file}: {file_batches} batches, {file_size:,} bytes{marker}")
            
            safe_print(f"\n📊 File totals: {total_file_batches} batches, {total_file_size:,} bytes ({total_file_size/1024/1024:.2f} MB)")
            safe_print(f"🟢 Current/latest file: {get_latest_batch_file()}")
        
        # Tampilkan format file
        safe_print(f"\n📋 File format: {BATCH_COLUMNS}")
        
        # Tampilkan 5 batch pertama dan terakhir dengan verifikasi
        safe_print(f"\n📋 First 5 batches (with xiebo compatibility check):")
        sorted_ids = sorted([int(id) for id in batch_dict.keys() if id.isdigit()])
        for i in range(min(5, len(sorted_ids))):
            batch_id = str(sorted_ids[i])
            batch = batch_dict[batch_id]
            is_compatible = verify_xiebo_compatibility_silent(batch['start_hex'], batch['end_hex'])
            status = "✅" if is_compatible else "⚠️"
            safe_print(f"  {status} ID: {batch_id}, Start: 0x{batch['start_hex']}, End: 0x{batch['end_hex']}")
        
        if len(sorted_ids) > 5:
            safe_print(f"\n📋 Last 5 batches (with xiebo compatibility check):")
            for i in range(max(0, len(sorted_ids)-5), len(sorted_ids)):
                batch_id = str(sorted_ids[i])
                batch = batch_dict[batch_id]
                is_compatible = verify_xiebo_compatibility_silent(batch['start_hex'], batch['end_hex'])
                status = "✅" if is_compatible else "⚠️"
                safe_print(f"  {status} ID: {batch_id}, Start: 0x{batch['start_hex']}, End: 0x{batch['end_hex']}")
        
        # Info next batch jika ada
        next_info = load_next_batch_info()
        if next_info:
            safe_print(f"\n💾 NEXT BATCH INFO:")
            safe_print(f"  Next start: 0x{next_info.get('next_start_hex')}")
            safe_print(f"  Progress: {next_info.get('batches_generated')}/{next_info.get('total_batches')} batches")
            safe_print(f"  Current batch file: {next_info.get('current_batch_file', 'Unknown')}")
            safe_print(f"  Current batch index: {next_info.get('current_batch_index', 'Unknown')}")
            safe_print(f"  To continue: python3 genb.py --continue")
        else:
            safe_print(f"\n✅ All batches completed!")
        
        safe_print(f"{'='*60}")
        
    except Exception as e:
        safe_print(f"❌ Error displaying summary: {e}")

def export_to_csv(output_file="batches.csv"):
    """Export batch data ke format CSV untuk analisis"""
    batch_dict = read_all_batches_as_dict()
    
    if len(batch_dict) == 0:
        safe_print("❌ No batch data found to export")
        return
    
    try:
        # Filter hanya batch dengan ID numerik
        numeric_batches = {}
        for batch_id, batch in batch_dict.items():
            if batch_id.isdigit():
                numeric_batches[int(batch_id)] = batch
        
        if len(numeric_batches) == 0:
            safe_print("❌ No numeric batch data to export")
            return
        
        # Sort by batch ID
        sorted_ids = sorted(numeric_batches.keys())
        
        # Write to CSV
        with open(output_file, 'w', newline='') as f:
            # Gunakan fieldnames yang sama
            writer = csv.DictWriter(f, fieldnames=BATCH_COLUMNS)
            writer.writeheader()
            
            for batch_id in sorted_ids:
                writer.writerow(numeric_batches[str(batch_id)])
        
        safe_print(f"✅ Exported {len(numeric_batches)} batches to {output_file}")
        safe_print(f"   File size: {os.path.getsize(output_file):,} bytes")
        
    except Exception as e:
        safe_print(f"❌ Error exporting to CSV: {e}")

def display_file_info():
    """Menampilkan informasi semua file batch"""
    # Cari semua file batch
    batch_files = []
    for file in os.listdir('.'):
        if file.startswith(LOG_FILE_PREFIX) and file.endswith(LOG_FILE_EXT):
            batch_files.append(file)
    
    if not batch_files:
        safe_print("📭 No batch files found")
        return
    
    batch_files.sort()
    
    safe_print(f"\n{'='*60}")
    safe_print(f"📁 BATCH FILES INFORMATION (Xiebo Compatible)")
    safe_print(f"{'='*60}")
    
    total_size = 0
    total_batches = 0
    total_xiebo_compatible = 0
    
    for file in batch_files:
        try:
            file_size = os.path.getsize(file)
            total_size += file_size
            
            # Hitung jumlah batch dalam file
            batch_count = 0
            xiebo_compatible_count = 0
            if os.path.exists(file):
                with open(file, 'r') as f:
                    reader = csv.DictReader(f, delimiter='|')
                    for row in reader:
                        batch_count += 1
                        if verify_xiebo_compatibility_silent(row['start_hex'], row['end_hex']):
                            xiebo_compatible_count += 1
                
                total_batches += batch_count
                total_xiebo_compatible += xiebo_compatible_count
            
            marker = " 🟢" if file == get_latest_batch_file() else ""
            compatibility_rate = xiebo_compatible_count/batch_count*100 if batch_count > 0 else 0
            
            safe_print(f"\n📄 {file}{marker}:")
            safe_print(f"   Size: {file_size:,} bytes ({file_size/1024:.2f} KB)")
            safe_print(f"   Batches: {batch_count}")
            safe_print(f"   Xiebo compatible: {xiebo_compatible_count}/{batch_count} ({compatibility_rate:.1f}%)")
            safe_print(f"   Format: {BATCH_COLUMNS}")
            
        except Exception as e:
            safe_print(f"❌ Error reading {file}: {e}")
    
    safe_print(f"\n{'='*60}")
    safe_print(f"📊 TOTAL SUMMARY:")
    safe_print(f"   Files: {len(batch_files)}")
    safe_print(f"   Total size: {total_size:,} bytes ({total_size/1024/1024:.2f} MB)")
    safe_print(f"   Total batches: {total_batches}")
    safe_print(f"   Total xiebo compatible: {total_xiebo_compatible}/{total_batches} ({total_xiebo_compatible/total_batches*100:.1f}%)")
    safe_print(f"   Latest file: {get_latest_batch_file()}")
    safe_print(f"   Threads available: {MAX_THREADS}")
    safe_print(f"{'='*60}")
    
    # Info next batch jika ada
    next_info = load_next_batch_info()
    if next_info:
        safe_print(f"\n💾 NEXT BATCH INFO:")
        safe_print(f"   Next start: 0x{next_info.get('next_start_hex')}")
        safe_print(f"   Progress: {next_info.get('batches_generated')}/{next_info.get('total_batches')} batches")
        safe_print(f"   Current file: {next_info.get('current_batch_file', 'Unknown')}")
        safe_print(f"   File index: {next_info.get('current_batch_index', 'Unknown')}")

def main():
    """Main function untuk generate batch"""
    global CURRENT_LOG_FILE, stop_monitor, BATCH_SIZE, MAX_THREADS
    
    print("\n" + "="*60)
    print("BATCH GENERATOR TOOL - XIEBO COMPATIBLE MODE")
    print("="*60)
    print("Tool untuk generate batch dari range hex")
    print(f"Output format: {BATCH_COLUMNS}")
    print(f"🔧 AUTO-ADJUST: Batch size adjusted to power of 2 for xiebo compatibility")
    print(f"✅ VERIFICATION: Checks xiebo compatibility for all batches")
    print(f"Auto-continue until completion (no confirmation)")
    print(f"Multiple batch files with auto-incrementing index")
    print(f"Smart Google Drive upload (only latest files)")
    print(f"🚀 MULTITHREADED with {MAX_THREADS} threads")
    print(f"🛡️  Thread-safe with proper cleanup")
    print("="*60)
    
    if len(sys.argv) < 2:
        print("\nUsage:")
        print("  Generate batches: python3 genb.py --generate START_HEX RANGE_BITS [ADDRESS]")
        print("  Continue generation (auto until completion, NO CONFIRMATION): python3 genb.py --continue")
        print("  Continue (single run): python3 genb.py --continue-single")
        print("  Continue (single run, single thread): python3 genb.py --continue-single-st")
        print("  Show summary: python3 genb.py --summary")
        print("  Export to CSV: python3 genb.py --export [filename.csv]")
        print("  Set batch size: python3 genb.py --set-size SIZE")
        print("  Set thread count: python3 genb.py --set-threads NUM")
        print("  File info: python3 genb.py --info")
        print("\nOptions:")
        print(f"  Default batch size: {BATCH_SIZE:,} keys")
        print(f"  Note: Batch size will be adjusted to nearest power of 2 for xiebo compatibility")
        print(f"  Default address: {DEFAULT_ADDRESS}")
        print(f"  Max batches per run: {MAX_BATCHES_PER_RUN:,}")
        print(f"  Max threads: {MAX_THREADS}")
        print(f"  Output columns: {BATCH_COLUMNS}")
        print(f"  Batch files: {LOG_FILE_PREFIX}_001.txt, {LOG_FILE_PREFIX}_002.txt, ...")
        print(f"  Auto-create new file when: file > 10MB or > 10,000 batches")
        print(f"  Google Drive: Only uploads latest batch file and nextbatch.txt")
        print(f"  --continue: Will run continuously WITHOUT asking for confirmation")
        print(f"  Press Ctrl+C to stop at any time")
        sys.exit(1)
    
    # Show file info mode
    if sys.argv[1] == "--info":
        display_file_info()
        sys.exit(0)
    
    # Show summary mode
    elif sys.argv[1] == "--summary":
        display_batch_summary()
        sys.exit(0)
    
    # Export mode
    elif sys.argv[1] == "--export":
        if len(sys.argv) > 2:
            output_file = sys.argv[2]
        else:
            output_file = "batches.csv"
        export_to_csv(output_file)
        sys.exit(0)
    
    # Set batch size mode
    elif sys.argv[1] == "--set-size":
        if len(sys.argv) != 3:
            print("Usage: python3 genb.py --set-size SIZE")
            sys.exit(1)
        
        try:
            new_size = int(sys.argv[2])
            if new_size <= 0:
                print("❌ Batch size must be positive")
                sys.exit(1)
            
            # Update global BATCH_SIZE
            globals()['BATCH_SIZE'] = new_size
            
            # Show adjusted size for xiebo compatibility
            adjusted_size, bits = adjust_batch_size_for_xiebo(new_size)
            print(f"✅ Batch size set to {new_size:,} keys")
            print(f"📏 For xiebo compatibility, will use: {adjusted_size:,} keys (2^{bits})")
            print(f"   Difference: {adjusted_size - new_size:,} keys")
        except ValueError:
            print("❌ Invalid batch size. Must be an integer.")
            sys.exit(1)
        sys.exit(0)
    
    # Set thread count mode
    elif sys.argv[1] == "--set-threads":
        if len(sys.argv) != 3:
            print("Usage: python3 genb.py --set-threads NUM")
            sys.exit(1)
        
        try:
            new_threads = int(sys.argv[2])
            if new_threads <= 0 or new_threads > 64:
                print("❌ Thread count must be between 1 and 64")
                sys.exit(1)
            
            # Update global MAX_THREADS
            globals()['MAX_THREADS'] = new_threads
            print(f"✅ Thread count set to {new_threads}")
        except ValueError:
            print("❌ Invalid thread count. Must be an integer.")
            sys.exit(1)
        sys.exit(0)
    
    # Continue mode (single run, single thread)
    elif sys.argv[1] == "--continue-single-st":
        # Set current batch file
        CURRENT_LOG_FILE = get_current_batch_file()
        print(f"📁 Current batch file: {CURRENT_LOG_FILE}")
        print(f"⚠️  Using SINGLE THREAD mode")
        
        continue_generation_single(BATCH_SIZE, MAX_BATCHES_PER_RUN, use_multithread=False)
        sys.exit(0)
    
    # Continue mode (single run, multithreaded)
    elif sys.argv[1] == "--continue-single":
        # Set current batch file
        CURRENT_LOG_FILE = get_current_batch_file()
        print(f"📁 Current batch file: {CURRENT_LOG_FILE}")
        print(f"🚀 Using MULTITHREADED mode with {MAX_THREADS} threads")
        
        continue_generation_single(BATCH_SIZE, MAX_BATCHES_PER_RUN, use_multithread=True)
        sys.exit(0)
    
    # Continue mode (auto - until completion, NO CONFIRMATION)
    elif sys.argv[1] == "--continue":
        CURRENT_LOG_FILE = get_current_batch_file()
        print(f"📁 Starting with batch file: {CURRENT_LOG_FILE}")
        print(f"🚀 Using MULTITHREADED mode with {MAX_THREADS} threads")
        print(f"🔧 Batch size will be adjusted to power of 2 for xiebo compatibility")
        print(f"⚠️  WARNING: Auto-continue mode activated. Process will run until completion WITHOUT confirmation.")
        print(f"   Press Ctrl+C to stop at any time.\n")
        
        try:
            continue_generation_auto(BATCH_SIZE, MAX_BATCHES_PER_RUN)
        except KeyboardInterrupt:
            print("\n\n⚠️ Process stopped by user")
        except Exception as e:
            print(f"\n❌ Error: {e}")
        
        # Cleanup sebelum exit
        cleanup_threads()
        sys.exit(0)
    
    # Generate mode
    elif sys.argv[1] == "--generate":
        if len(sys.argv) < 4:
            print("Usage: python3 genb.py --generate START_HEX RANGE_BITS [ADDRESS]")
            sys.exit(1)
        
        start_hex = sys.argv[2]
        range_bits = int(sys.argv[3])
        
        if len(sys.argv) > 4:
            address = sys.argv[4]
        else:
            address = DEFAULT_ADDRESS
        
        # Validasi input
        try:
            start_int = int(start_hex, 16)
            if start_int < 0:
                print("❌ Start hex must be positive")
                sys.exit(1)
        except ValueError:
            print("❌ Invalid start hex format")
            sys.exit(1)
        
        if range_bits <= 0 or range_bits > 256:
            print("❌ Range bits must be between 1 and 256")
            sys.exit(1)
        
        # Set current batch file (akan dibuat baru)
        CURRENT_LOG_FILE = None
        
        print(f"🚀 Using MULTITHREADED mode with {MAX_THREADS} threads")
        print(f"🔧 Batch size will be adjusted to power of 2 for xiebo compatibility")
        
        try:
            # Generate batches menggunakan multithreading
            total_batches_needed, batches_generated, _ = generate_batches_multithreaded(
                start_hex, range_bits, address, BATCH_SIZE, max_batches=MAX_BATCHES_PER_RUN
            )
            
            print(f"\n{'='*60}")
            print(f"✅ GENERATION COMPLETED")
            print(f"{'='*60}")
            print(f"Generated {batches_generated} batches")
            print(f"Total batches needed: {total_batches_needed}")
            print(f"File: {CURRENT_LOG_FILE} (format: {BATCH_COLUMNS})")
            
            # Tampilkan ukuran file
            if CURRENT_LOG_FILE and os.path.exists(CURRENT_LOG_FILE):
                file_size = os.path.getsize(CURRENT_LOG_FILE)
                print(f"File size: {file_size:,} bytes")
            
            if batches_generated < total_batches_needed:
                print(f"Batches remaining: {total_batches_needed - batches_generated}")
                print(f"To continue (auto, no confirmation): python3 genb.py --continue")
                print(f"To continue single run (multithreaded): python3 genb.py --continue-single")
                print(f"To continue single run (single thread): python3 genb.py --continue-single-st")
            
            display_batch_summary()
            
        except KeyboardInterrupt:
            print("\n\n⚠️ Generation interrupted by user")
        except Exception as e:
            print(f"\n❌ Error: {e}")
        
        # Cleanup sebelum exit
        cleanup_threads()
        
    else:
        print("❌ Invalid command")
        print("Usage: python3 genb.py --generate START_HEX RANGE_BITS [ADDRESS]")
        print("Or:    python3 genb.py --continue (auto until completion, NO CONFIRMATION)")
        print("Or:    python3 genb.py --continue-single (single run, multithreaded)")
        print("Or:    python3 genb.py --continue-single-st (single run, single thread)")
        print("Or:    python3 genb.py --summary")
        print("Or:    python3 genb.py --info")
        sys.exit(1)

if __name__ == "__main__":
    main()
