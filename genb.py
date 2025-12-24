import sys
import os
import math
from datetime import datetime
import csv

# Konfigurasi file log
LOG_FILE = "generated_batches.txt"
NEXT_BATCH_FILE = "nextbatch.txt"  # File untuk menyimpan start range berikutnya
DRIVE_MOUNT_PATH = "/content/drive"
DRIVE_FILE_PATH = "/content/drive/MyDrive/generated_batches.txt"
DRIVE_NEXT_BATCH_PATH = "/content/drive/MyDrive/nextbatch.txt"

# Kolom-kolom untuk tabel batch (hanya 2 kolom)
BATCH_COLUMNS = [
    'batch_id',
    'start_hex',
    'end_hex'
]

# Konfigurasi batch - sebagai variabel module-level
MAX_BATCHES_PER_RUN = 1000000  # Maksimal 1juta batch per eksekusi
BATCH_SIZE = 2000000000000  # 2 triliun keys per batch (default)
DEFAULT_ADDRESS = "N/A"  # Default address untuk batch generation

def save_to_drive():
    """Menyimpan file ke Google Drive"""
    try:
        # Cek apakah Google Drive tersedia (untuk Google Colab)
        if os.path.exists(DRIVE_MOUNT_PATH):
            from google.colab import drive
            import shutil
            
            # Mount drive jika belum
            if not os.path.exists(os.path.join(DRIVE_MOUNT_PATH, "MyDrive")):
                drive.mount(DRIVE_MOUNT_PATH, force_remount=False)
            
            # Salin file generated_batches.txt
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

def read_batches_as_dict():
    """Membaca batch file dan mengembalikan dictionary berdasarkan batch_id"""
    batch_dict = {}
    
    if not os.path.exists(LOG_FILE):
        return batch_dict
    
    try:
        with open(LOG_FILE, 'r') as f:
            # Baca header
            reader = csv.DictReader(f, delimiter='|')
            for row in reader:
                batch_id = row.get('batch_id', '').strip()
                if batch_id:
                    batch_dict[batch_id] = row
    except Exception as e:
        print(f"⚠️ Error reading batch file: {e}")
    
    return batch_dict

def write_batches_from_dict(batch_dict):
    """Menulis batch file dari dictionary"""
    try:
        # Konversi dictionary ke list
        rows = []
        for batch_id in sorted(batch_dict.keys(), key=lambda x: int(x) if x.isdigit() else x):
            rows.append(batch_dict[batch_id])
        
        # Tulis ke file dengan format tabel
        with open(LOG_FILE, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=BATCH_COLUMNS, delimiter='|')
            writer.writeheader()
            writer.writerows(rows)
        
        # Simpan ke Google Drive (silent)
        save_to_drive()
        
    except Exception as e:
        print(f"❌ Error writing batch file: {e}")

def save_next_batch_info(start_hex, range_bits, address, next_start_hex, batches_generated, total_batches, timestamp=None):
    """Menyimpan informasi batch berikutnya ke file"""
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
            'timestamp': timestamp
        }
        
        # 1. Simpan ke file nextbatch.txt
        with open(NEXT_BATCH_FILE, 'w') as f:
            for key, value in info.items():
                f.write(f"{key}={value}\n")
        
        # 2. Simpan ke Google Drive
        save_to_drive()
        
        print(f"📝 Next batch info saved:")
        print(f"   File: {NEXT_BATCH_FILE}")
        print(f"   Next start: 0x{next_start_hex}")
        print(f"   Progress: {batches_generated}/{total_batches} batches generated")
        
    except Exception as e:
        print(f"❌ Error saving next batch info: {e}")

def load_next_batch_info():
    """Memuat informasi batch berikutnya dari file"""
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
        
        return info
    except Exception as e:
        print(f"❌ Error loading next batch info from file: {e}")
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

def generate_batches(start_hex, range_bits, address, batch_size, start_batch_id=0, max_batches=None):
    """Generate batch dari range hex"""
    
    start_int = int(start_hex, 16)
    total_keys = 1 << range_bits
    end_int = start_int + total_keys - 1
    
    total_batches_needed = math.ceil(total_keys / batch_size)
    
    # Limit jumlah batch jika ada max_batches
    if max_batches is not None:
        batches_to_generate = min(total_batches_needed, max_batches)
    else:
        batches_to_generate = total_batches_needed
    
    print(f"\n{'='*60}")
    print(f"GENERATING BATCHES")
    print(f"{'='*60}")
    print(f"Start: 0x{start_hex}")
    print(f"Range: {range_bits} bits")
    print(f"Total keys: {total_keys:,}")
    print(f"End: 0x{format(end_int, 'x')}")
    print(f"Batch size: {batch_size:,} keys")
    print(f"Address: {address}")
    print(f"Total batches needed: {total_batches_needed:,}")
    print(f"Batches to generate: {batches_to_generate}")
    print(f"Starting batch ID: {start_batch_id}")
    print(f"Output format: {BATCH_COLUMNS}")
    print(f"{'='*60}")
    
    batch_dict = {}
    
    for i in range(batches_to_generate):
        batch_id = start_batch_id + i
        batch_start = start_int + (i * batch_size)
        batch_end = min(batch_start + batch_size, end_int + 1)
        batch_keys = batch_end - batch_start
        
        # Hitung bits untuk batch ini (untuk display saja, tidak disimpan)
        batch_bits = calculate_range_bits(batch_keys)
        
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
            print(f"✅ Generated batch {i+1}/{batches_to_generate}: ID={batch_id}, Start=0x{batch_start_hex}, End=0x{batch_end_hex}, Keys={batch_keys:,}")
    
    # Tulis batch ke file
    write_batches_from_dict(batch_dict)
    
    # Simpan info batch berikutnya jika belum selesai semua
    if batches_to_generate < total_batches_needed:
        next_start_int = start_int + (batches_to_generate * batch_size)
        next_start_hex = format(next_start_int, 'x')
        
        save_next_batch_info(
            start_hex,
            range_bits,
            address,
            next_start_hex,
            batches_to_generate,
            total_batches_needed
        )
    
    return total_batches_needed, batches_to_generate, batch_dict

def display_batch_summary():
    """Menampilkan summary batch yang telah digenerate"""
    if not os.path.exists(LOG_FILE):
        print("📭 No batch file found")
        return
    
    try:
        batch_dict = read_batches_as_dict()
        total_batches = len(batch_dict)
        
        if total_batches == 0:
            print("📭 No batches in file")
            return
        
        print(f"\n{'='*60}")
        print(f"📊 BATCH SUMMARY")
        print(f"{'='*60}")
        print(f"Total batches generated: {total_batches}")
        print(f"File size: {os.path.getsize(LOG_FILE):,} bytes")
        
        # Tampilkan format file
        print(f"\n📋 File format: {BATCH_COLUMNS}")
        
        # Tampilkan 5 batch pertama dan terakhir
        print(f"\n📋 First 5 batches:")
        sorted_ids = sorted([int(id) for id in batch_dict.keys() if id.isdigit()])
        for i in range(min(5, len(sorted_ids))):
            batch_id = str(sorted_ids[i])
            batch = batch_dict[batch_id]
            print(f"  ID: {batch_id}, Start: 0x{batch['start_hex']}, End: 0x{batch['end_hex']}")
        
        if len(sorted_ids) > 5:
            print(f"\n📋 Last 5 batches:")
            for i in range(max(0, len(sorted_ids)-5), len(sorted_ids)):
                batch_id = str(sorted_ids[i])
                batch = batch_dict[batch_id]
                print(f"  ID: {batch_id}, Start: 0x{batch['start_hex']}, End: 0x{batch['end_hex']}")
        
        # Info next batch jika ada
        next_info = load_next_batch_info()
        if next_info:
            print(f"\n💾 NEXT BATCH INFO:")
            print(f"  Next start: 0x{next_info.get('next_start_hex')}")
            print(f"  Progress: {next_info.get('batches_generated')}/{next_info.get('total_batches')} batches")
            print(f"  To continue: python3 genb.py --continue")
        
        print(f"{'='*60}")
        
    except Exception as e:
        print(f"❌ Error displaying summary: {e}")

def continue_generation(batch_size, max_batches=None):
    """Lanjutkan generate batch dari state yang tersimpan"""
    next_info = load_next_batch_info()
    if not next_info:
        print("❌ No saved state found. Run with --generate first.")
        sys.exit(1)
    
    print(f"\n{'='*60}")
    print(f"CONTINUE GENERATION")
    print(f"{'='*60}")
    
    start_hex = next_info['next_start_hex']
    range_bits = int(next_info['original_range_bits'])
    address = next_info['address']
    batches_generated = int(next_info['batches_generated'])
    total_batches = int(next_info['total_batches'])
    
    print(f"Resuming from saved state...")
    print(f"Next start: 0x{start_hex}")
    print(f"Range: {range_bits} bits")
    print(f"Address: {address}")
    print(f"Batches already generated: {batches_generated}")
    print(f"Total batches needed: {total_batches}")
    print(f"Timestamp: {next_info.get('timestamp', 'unknown')}")
    print(f"Output format: {BATCH_COLUMNS}")
    print(f"{'='*60}")
    
    # Hitung jumlah batch yang tersisa
    remaining_batches = total_batches - batches_generated
    
    # Limit jumlah batch jika ada max_batches
    if max_batches is not None:
        batches_to_generate = min(remaining_batches, max_batches)
    else:
        batches_to_generate = remaining_batches
    
    if batches_to_generate <= 0:
        print("✅ All batches already generated!")
        return
    
    print(f"\nGenerating {batches_to_generate} more batches")
    print(f"{remaining_batches} batches remaining in total")
    
    # Generate batch
    total_batches_needed, actual_generated, batch_dict = generate_batches(
        start_hex, range_bits, address, batch_size, 
        start_batch_id=batches_generated, max_batches=batches_to_generate
    )
    
    print(f"\n{'='*60}")
    print(f"✅ GENERATION COMPLETED")
    print(f"{'='*60}")
    print(f"Generated {actual_generated} new batches")
    print(f"Total batches generated so far: {batches_generated + actual_generated}/{total_batches}")
    
    # Update state
    if batches_generated + actual_generated < total_batches:
        print(f"\n💾 State updated for next run")
        print(f"   To continue: python3 genb.py --continue")
    
    display_batch_summary()

def export_to_csv(output_file="batches.csv"):
    """Export batch data ke format CSV untuk analisis"""
    if not os.path.exists(LOG_FILE):
        print("❌ No batch file found to export")
        return
    
    try:
        batch_dict = read_batches_as_dict()
        
        # Filter hanya batch dengan ID numerik
        numeric_batches = {}
        for batch_id, batch in batch_dict.items():
            if batch_id.isdigit():
                numeric_batches[int(batch_id)] = batch
        
        if len(numeric_batches) == 0:
            print("❌ No numeric batch data to export")
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
        
        print(f"✅ Exported {len(numeric_batches)} batches to {output_file}")
        print(f"   File size: {os.path.getsize(output_file):,} bytes")
        
    except Exception as e:
        print(f"❌ Error exporting to CSV: {e}")

def display_file_info():
    """Menampilkan informasi file generated_batches.txt"""
    if not os.path.exists(LOG_FILE):
        print("📭 No generated_batches.txt file found")
        return
    
    try:
        file_size = os.path.getsize(LOG_FILE)
        print(f"\n📁 File: {LOG_FILE}")
        print(f"📏 Size: {file_size:,} bytes ({file_size/1024:.2f} KB)")
        
        # Baca beberapa baris pertama
        print(f"\n📋 First 3 lines:")
        with open(LOG_FILE, 'r') as f:
            for i in range(4):  # Header + 3 data
                line = f.readline()
                if line:
                    print(f"  {i+1}: {line.strip()}")
        
        # Hitung jumlah batch
        batch_dict = read_batches_as_dict()
        total_batches = len([id for id in batch_dict.keys() if id.isdigit()])
        print(f"\n📊 Total batches in file: {total_batches}")
        
    except Exception as e:
        print(f"❌ Error displaying file info: {e}")

def main():
    """Main function untuk generate batch"""
    
    print("\n" + "="*60)
    print("BATCH GENERATOR TOOL - MINIMAL FORMAT")
    print("="*60)
    print("Tool untuk generate batch dari range hex")
    print(f"Output format: {BATCH_COLUMNS}")
    print(f"File size optimized (minimal columns)")
    print("="*60)
    
    if len(sys.argv) < 2:
        print("\nUsage:")
        print("  Generate batches: python3 genb.py --generate START_HEX RANGE_BITS [ADDRESS]")
        print("  Continue generation: python3 genb.py --continue")
        print("  Show summary: python3 genb.py --summary")
        print("  Export to CSV: python3 genb.py --export [filename.csv]")
        print("  Set batch size: python3 genb.py --set-size SIZE")
        print("  File info: python3 genb.py --info")
        print("\nOptions:")
        print(f"  Default batch size: {BATCH_SIZE:,} keys")
        print(f"  Default address: {DEFAULT_ADDRESS}")
        print(f"  Max batches per run: {MAX_BATCHES_PER_RUN}")
        print(f"  Output columns: {BATCH_COLUMNS}")
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
    
    # Continue mode
    elif sys.argv[1] == "--continue":
        continue_generation(BATCH_SIZE, MAX_BATCHES_PER_RUN)
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
        
        # Generate batches
        total_batches_needed, batches_generated, _ = generate_batches(
            start_hex, range_bits, address, BATCH_SIZE, max_batches=MAX_BATCHES_PER_RUN
        )
        
        print(f"\n{'='*60}")
        print(f"✅ GENERATION COMPLETED")
        print(f"{'='*60}")
        print(f"Generated {batches_generated} batches")
        print(f"Total batches needed: {total_batches_needed}")
        print(f"File: {LOG_FILE} (format: {BATCH_COLUMNS})")
        
        # Tampilkan ukuran file
        if os.path.exists(LOG_FILE):
            file_size = os.path.getsize(LOG_FILE)
            print(f"File size: {file_size:,} bytes")
        
        if batches_generated < total_batches_needed:
            print(f"Batches remaining: {total_batches_needed - batches_generated}")
            print(f"To continue: python3 genb.py --continue")
        
        display_batch_summary()
        
    else:
        print("❌ Invalid command")
        print("Usage: python3 genb.py --generate START_HEX RANGE_BITS [ADDRESS]")
        print("Or:    python3 genb.py --continue")
        print("Or:    python3 genb.py --summary")
        print("Or:    python3 genb.py --info")
        sys.exit(1)

if __name__ == "__main__":
    main()
