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

def write_batches_from_dict(batch_dict, create_new_file=False):
    """Menulis batch file dari dictionary"""
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
        
        # Tulis ke file dengan format tabel
        with open(CURRENT_LOG_FILE, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=BATCH_COLUMNS, delimiter='|')
            writer.writeheader()
            writer.writerows(rows)
        
        safe_print(f"💾 Batch data saved to: {CURRENT_LOG_FILE}")
        safe_print(f"📊 Total batches in file: {len(rows)}")
        
        # Simpan ke Google Drive (dengan feedback) - HANYA upload file ini
        safe_print(f"🔄 Saving to Google Drive...")
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
        
        # Set current batch file berdasarkan current_batch_index jika ada
        if 'current_batch_index' in info:
            try:
                batch_index = int(info['current_batch_index'])
                expected_file = f"{LOG_FILE_PREFIX}_{batch_index:03d}{LOG_FILE_EXT}"
                
                # Jika file yang diharapkan ada, gunakan itu
                if os.path.exists(expected_file):
                    CURRENT_LOG_FILE = expected_file
                else:
                    # Coba gunakan file terbaru yang ada
                    latest_file = get_latest_batch_file()
                    if latest_file:
                        CURRENT_LOG_FILE = latest_file
                    else:
                        # Buat file baru berdasarkan index
                        CURRENT_LOG_FILE = expected_file
            except (ValueError, KeyError) as e:
                safe_print(f"⚠️ Error parsing current_batch_index: {e}")
                # Fallback ke current_batch_file jika ada
                if 'current_batch_file' in info:
                    CURRENT_LOG_FILE = info['current_batch_file']
        elif 'current_batch_file' in info:
            CURRENT_LOG_FILE = info['current_batch_file']
        
        return info
    except Exception as e:
        safe_print(f"❌ Error loading next batch info from file: {e}")
        return None

def calculate_range_bits(keys_count):
    """Menghitung range bits yang benar untuk jumlah keys tertentu"""
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

def generate_batch_worker(args):
    """Worker function untuk generate batch dalam thread"""
    start_int, batch_size, end_int, start_batch_id, i = args
    batch_id = start_batch_id + i
    batch_start = start_int + (i * batch_size)
    batch_end = min(batch_start + batch_size, end_int + 1)
    batch_keys = batch_end - batch_start
    
    batch_start_hex = format(batch_start, 'x')
    batch_end_hex = format(batch_end - 1, 'x')  # -1 karena end inklusif
    
    # Buat informasi batch (hanya 2 kolom)
    batch_info = {
        'batch_id': str(batch_id),
        'start_hex': batch_start_hex,
        'end_hex': batch_end_hex
    }
    
    return batch_id, batch_info, batch_keys, i

def generate_batches_multithreaded(start_hex, range_bits, address, batch_size, start_batch_id=0, max_batches=None):
    """Generate batch dari range hex menggunakan multithreading"""
    global CURRENT_LOG_FILE, stop_monitor
    
    start_int = int(start_hex, 16)
    total_keys = 1 << range_bits
    end_int = start_int + total_keys - 1
    
    total_batches_needed = math.ceil(total_keys / batch_size)
    
    # Limit jumlah batch jika ada max_batches
    if max_batches is not None:
        batches_to_generate = min(total_batches_needed, max_batches)
    else:
        batches_to_generate = total_batches_needed
    
    safe_print(f"\n{'='*60}")
    safe_print(f"GENERATING BATCHES - MULTITHREADED ({MAX_THREADS} threads)")
    safe_print(f"{'='*60}")
    safe_print(f"Start: 0x{start_hex}")
    safe_print(f"Range: {range_bits} bits")
    safe_print(f"Total keys: {total_keys:,}")
    safe_print(f"End: 0x{format(end_int, 'x')}")
    safe_print(f"Batch size: {batch_size:,} keys")
    safe_print(f"Address: {address}")
    safe_print(f"Total batches needed: {total_batches_needed:,}")
    safe_print(f"Batches to generate: {batches_to_generate}")
    safe_print(f"Starting batch ID: {start_batch_id}")
    safe_print(f"Output format: {BATCH_COLUMNS}")
    safe_print(f"Output file: {CURRENT_LOG_FILE if CURRENT_LOG_FILE else 'Auto-determined'}")
    safe_print(f"Threads: {MAX_THREADS}")
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
                            batch_id, batch_idx, batch_keys = progress_queue.get_nowait()
                            completed_count += 1
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
    
    try:
        # Gunakan ThreadPoolExecutor untuk parallel processing
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            # Prepare arguments untuk semua batch
            batch_args = [(start_int, batch_size, end_int, start_batch_id, i) 
                         for i in range(batches_to_generate)]
            
            # Submit semua tasks
            futures = [executor.submit(generate_batch_worker, arg) for arg in batch_args]
            
            # Process hasil
            for future in as_completed(futures):
                try:
                    batch_id, batch_info, batch_keys, batch_idx = future.result()
                    
                    # Simpan hasil ke dictionary dengan lock untuk thread safety
                    with file_lock:
                        batch_dict[str(batch_id)] = batch_info
                    
                    # Kirim progress update ke queue
                    progress_queue.put((batch_id, batch_idx, batch_keys))
                    
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
    
    safe_print(f"\n⏱️  Generation statistics:")
    safe_print(f"   Total time: {elapsed_time:.2f} seconds")
    safe_print(f"   Batches per second: {batches_per_second:.2f}")
    safe_print(f"   Threads used: {MAX_THREADS}")
    
    # Tulis batch ke file
    write_batches_from_dict(batch_dict, create_new_file)
    
    # Simpan info batch berikutnya jika belum selesai semua
    if batches_to_generate < total_batches_needed:
        next_start_int = start_int + (batches_to_generate * batch_size)
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
    total_keys = 1 << range_bits
    end_int = start_int + total_keys - 1
    
    total_batches_needed = math.ceil(total_keys / batch_size)
    
    # Limit jumlah batch jika ada max_batches
    if max_batches is not None:
        batches_to_generate = min(total_batches_needed, max_batches)
    else:
        batches_to_generate = total_batches_needed
    
    safe_print(f"\n{'='*60}")
    safe_print(f"GENERATING BATCHES - SINGLE THREAD")
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
    
    start_time = time.time()
    
    for i in range(batches_to_generate):
        batch_id = start_batch_id + i
        batch_start = start_int + (i * batch_size)
        batch_end = min(batch_start + batch_size, end_int + 1)
        batch_keys = batch_end - batch_start
        
        batch_start_hex = format(batch_start, 'x')
        batch_end_hex = format(batch_end - 1, 'x')  # -1 karena end inklusif
        
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
            safe_print(f"✅ Generated batch {i+1}/{batches_to_generate}: ID={batch_id}, "
                      f"Speed={batches_per_sec:.1f} batches/sec")
    
    elapsed_time = time.time() - start_time
    safe_print(f"\n⏱️  Generation time: {elapsed_time:.2f} seconds")
    
    # Tulis batch ke file
    write_batches_from_dict(batch_dict, create_new_file)
    
    # Simpan info batch berikutnya jika belum selesai semua
    if batches_to_generate < total_batches_needed:
        next_start_int = start_int + (batches_to_generate * batch_size)
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
        safe_print(f"📊 BATCH SUMMARY")
        safe_print(f"{'='*60}")
        safe_print(f"Total batches generated: {total_batches}")
        safe_print(f"Total batch files: {len(batch_files)}")
        
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
        
        # Tampilkan 5 batch pertama dan terakhir
        safe_print(f"\n📋 First 5 batches:")
        sorted_ids = sorted([int(id) for id in batch_dict.keys() if id.isdigit()])
        for i in range(min(5, len(sorted_ids))):
            batch_id = str(sorted_ids[i])
            batch = batch_dict[batch_id]
            safe_print(f"  ID: {batch_id}, Start: 0x{batch['start_hex']}, End: 0x{batch['end_hex']}")
        
        if len(sorted_ids) > 5:
            safe_print(f"\n📋 Last 5 batches:")
            for i in range(max(0, len(sorted_ids)-5), len(sorted_ids)):
                batch_id = str(sorted_ids[i])
                batch = batch_dict[batch_id]
                safe_print(f"  ID: {batch_id}, Start: 0x{batch['start_hex']}, End: 0x{batch['end_hex']}")
        
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

def continue_generation_auto(batch_size, max_batches=None):
    """Lanjutkan generate batch dari state yang tersimpan secara otomatis sampai selesai TANPA KONFIRMASI"""
    global CURRENT_LOG_FILE, stop_monitor
    
    run_count = 0
    
    try:
        while True:
            # Pemeriksaan awal: apakah ada file nextbatch.txt?
            if not os.path.exists(NEXT_BATCH_FILE):
                safe_print("❌ No nextbatch.txt file found. Cannot continue.")
                safe_print("   Please run with --generate first or ensure nextbatch.txt exists.")
                break
            
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
            current_batch_index = int(next_info.get('current_batch_index', 0))
            
            # Set current file berdasarkan current_batch_index
            if current_batch_index > 0:
                expected_file = f"{LOG_FILE_PREFIX}_{current_batch_index:03d}{LOG_FILE_EXT}"
                if os.path.exists(expected_file):
                    CURRENT_LOG_FILE = expected_file
                else:
                    # Cari file batch terbaru
                    latest_file = get_latest_batch_file()
                    if latest_file:
                        CURRENT_LOG_FILE = latest_file
                        safe_print(f"⚠️ Expected file {expected_file} not found, using latest: {latest_file}")
                    else:
                        # Buat file baru berdasarkan index
                        CURRENT_LOG_FILE = expected_file
                        safe_print(f"📁 Creating new batch file: {CURRENT_LOG_FILE}")
            else:
                # Fallback ke current_batch_file jika ada
                current_file = next_info.get('current_batch_file', CURRENT_LOG_FILE)
                CURRENT_LOG_FILE = current_file
            
            safe_print(f"Resuming from saved state...")
            safe_print(f"Next start: 0x{start_hex}")
            safe_print(f"Range: {range_bits} bits")
            safe_print(f"Address: {address}")
            safe_print(f"Batches already generated: {batches_generated:,}")
            safe_print(f"Total batches needed: {total_batches:,}")
            safe_print(f"Current batch file: {CURRENT_LOG_FILE}")
            safe_print(f"Current batch index: {current_batch_index}")
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
    
    # Pemeriksaan awal: apakah ada file nextbatch.txt?
    if not os.path.exists(NEXT_BATCH_FILE):
        safe_print("❌ No nextbatch.txt file found. Cannot continue.")
        safe_print("   Please run with --generate first or ensure nextbatch.txt exists.")
        sys.exit(1)
    
    next_info = load_next_batch_info()
    if not next_info:
        safe_print("❌ No saved state found. Run with --generate first.")
        sys.exit(1)
    
    start_hex = next_info['next_start_hex']
    range_bits = int(next_info['original_range_bits'])
    address = next_info['address']
    batches_generated = int(next_info['batches_generated'])
    total_batches = int(next_info['total_batches'])
    current_batch_index = int(next_info.get('current_batch_index', 0))
    
    # Set current file berdasarkan current_batch_index
    if current_batch_index > 0:
        expected_file = f"{LOG_FILE_PREFIX}_{current_batch_index:03d}{LOG_FILE_EXT}"
        if os.path.exists(expected_file):
            CURRENT_LOG_FILE = expected_file
        else:
            # Cari file batch terbaru
            latest_file = get_latest_batch_file()
            if latest_file:
                CURRENT_LOG_FILE = latest_file
                safe_print(f"⚠️ Expected file {expected_file} not found, using latest: {latest_file}")
            else:
                # Buat file baru berdasarkan index
                CURRENT_LOG_FILE = expected_file
                safe_print(f"📁 Creating new batch file: {CURRENT_LOG_FILE}")
    else:
        # Fallback ke current_batch_file jika ada
        current_file = next_info.get('current_batch_file', CURRENT_LOG_FILE)
        CURRENT_LOG_FILE = current_file
    
    safe_print(f"\n{'='*60}")
    safe_print(f"CONTINUE GENERATION (SINGLE RUN)")
    safe_print(f"{'='*60}")
    safe_print(f"Current batch file: {CURRENT_LOG_FILE}")
    safe_print(f"Current batch index: {current_batch_index}")
    
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
    safe_print(f"📁 BATCH FILES INFORMATION")
    safe_print(f"{'='*60}")
    
    total_size = 0
    total_batches = 0
    
    for file in batch_files:
        try:
            file_size = os.path.getsize(file)
            total_size += file_size
            
            # Hitung jumlah batch dalam file
            batch_count = 0
            if os.path.exists(file):
                with open(file, 'r') as f:
                    reader = csv.DictReader(f, delimiter='|')
                    batch_count = sum(1 for _ in reader)
                total_batches += batch_count
            
            marker = " 🟢" if file == get_latest_batch_file() else ""
            safe_print(f"\n📄 {file}{marker}:")
            safe_print(f"   Size: {file_size:,} bytes ({file_size/1024:.2f} KB)")
            safe_print(f"   Batches: {batch_count}")
            
            # Tampilkan format
            safe_print(f"   Format: {BATCH_COLUMNS}")
            
        except Exception as e:
            safe_print(f"❌ Error reading {file}: {e}")
    
    safe_print(f"\n{'='*60}")
    safe_print(f"📊 TOTAL SUMMARY:")
    safe_print(f"   Files: {len(batch_files)}")
    safe_print(f"   Total size: {total_size:,} bytes ({total_size/1024/1024:.2f} MB)")
    safe_print(f"   Total batches: {total_batches}")
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
    global CURRENT_LOG_FILE, stop_monitor
    
    print("\n" + "="*60)
    print("BATCH GENERATOR TOOL - MINIMAL FORMAT")
    print("="*60)
    print("Tool untuk generate batch dari range hex")
    print(f"Output format: {BATCH_COLUMNS}")
    print(f"Auto-continue until completion (no confirmation)")
    print(f"Multiple batch files with auto-incrementing index")
    print(f"Smart Google Drive upload (only latest files)")
    print(f"🚀 MULTITHREADED with {MAX_THREADS} threads")
    print(f"🛡️  Thread-safe with proper cleanup")
    print("="*60)
    
    # Pemeriksaan awal saat program dijalankan
    if os.path.exists(NEXT_BATCH_FILE):
        safe_print("📄 nextbatch.txt file found. Ready to continue from saved state.")
        next_info = load_next_batch_info()
        if next_info:
            safe_print(f"   Current batch file: {next_info.get('current_batch_file', 'Unknown')}")
            safe_print(f"   Current batch index: {next_info.get('current_batch_index', 'Unknown')}")
            safe_print(f"   Progress: {next_info.get('batches_generated', '0')}/{next_info.get('total_batches', '0')}")
            safe_print(f"   Use: python3 genb.py --continue (to auto-continue)")
    else:
        safe_print("📭 No nextbatch.txt file found. Ready for new generation.")
        safe_print(f"   Use: python3 genb.py --generate START_HEX RANGE_BITS [ADDRESS]")
    
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
            print(f"✅ Batch size set to {new_size:,} keys")
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
