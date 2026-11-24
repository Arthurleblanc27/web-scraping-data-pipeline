# Web Scraping Data Pipeline

Standar struktur penyimpanan data mentah (*raw data*) hasil *web scraping* beserta alur validasi, pembersihan, dan ekspor ke format tabular siap analisis (`.csv` dan `.xlsx`).

## Struktur Repositori

```text
web-scraping-data-pipeline/
├── SKILL.md        # Spesifikasi lengkap standar pipeline & contoh implementasi Python
├── README.md       # Dokumentasi utama repositori
├── LICENSE         # Lisensi MIT
└── .gitignore      # Konfigurasi pengecualian direktori data & artefak Python
```

## Fitur & Standar Utama

1. **Pemilihan Format Penyimpanan Raw Data**
   - **JSON Lines (`.jsonl` / `.ndjson`)**: Standar utama untuk struktur semi-terstruktur/bersarang (*nested*) dan penulisan bertahap tanpa membebani memori.
   - **JSON (`.json`)**: Untuk kumpulan data kecil yang seluruhnya muat di memori.
   - **CSV / TSV (`.csv`, `.tsv`)**: Untuk struktur kolom datar (*flat*) dan seragam.
   - **Raw HTML / XML (`.html`, `.xml`)**: Untuk menyimpan respons mentah saat aturan *parsing* (XPath/CSS selector) masih sering berubah.
   - **Apache Parquet (`.parquet`)**: Untuk skala jutaan baris yang diproses menggunakan Pandas, Polars, DuckDB, atau Spark.

2. **Pemisahan Metadata Teknis dan Muatan Mentah**
   - Menjaga sifat *immutable* pada data mentah dengan memisahkan setiap rekaman menjadi tiga bagian utama:
     - `record_id`: Pengenal unik tiap baris (UUID v4).
     - `metadata`: Konteks pengambilan (`source_url`, `scraped_at` ISO 8601 + zona waktu, `http_status`, dan `scraper_version`).
     - `raw_payload`: Nilai mentah hasil ekstraksi halaman apa adanya tanpa pembersihan dini.

3. **Penyimpanan *Streaming* Tahan Gagal**
   - Menggunakan mode *append* (`"a"`), `encoding="utf-8"`, serta `ensure_ascii=False` tanpa `indent` agar setiap rekaman langsung tersimpan ke disk dan tahan terhadap gangguan koneksi maupun *rate limit*.
   - Mendukung kompresi `gzip` (`.jsonl.gz`) untuk volume ratusan ribu baris ke atas.

4. **Validasi, Pembersihan, dan Ekspor Tabular**
   - **Validasi toleran kesalahan**: Melewati baris rusak (`json.JSONDecodeError`) tanpa menghentikan pemrosesan seluruh file.
   - **Normalisasi aman**: Merapikan spasi berlebih, mengekstrak digit numerik, dan mempertahankan nilai kosong sebagai `None` (bukan `0`) agar tidak mendistorsi agregasi statistik.
   - **Deduplikasi berbasis sumber**: Mengurutkan berdasarkan `scraped_at` terbaru dan menghapus duplikasi berdasarkan `source_url`.
   - **Ekspor ramah spreadsheet**: Menulis CSV dengan `encoding="utf-8-sig"` agar karakter UTF-8 terbaca otomatis di Microsoft Excel (Windows) serta mendukung ekspor langsung ke `.xlsx`.

## Cara Penggunaan

### 1. Sebagai Agent Skill
Impor `web-scraping-data-pipeline.zip` atau salin `SKILL.md` ke direktori *skills* pada lingkungan agen AI Anda untuk dijadikan panduan standar perancangan dan pemrosesan data *scraping*.

### 2. Sebagai Referensi Skrip Python
Gunakan fungsi referensi di dalam [`SKILL.md`](SKILL.md):
- `append_jsonl_record()` untuk menyimpan hasil ekstraksi secara bertahap ke `data/raw/`.
- `load_jsonl_to_dataframe()` dan `export_tabular()` untuk membersihkan dan mengekspor hasil ke `data/processed/` (membutuhkan `pandas` dan `openpyxl`).

```bash
pip install pandas openpyxl
```

## Lisensi

Proyek ini dilisensikan di bawah [Lisensi MIT](LICENSE).
