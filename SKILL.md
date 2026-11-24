---
name: web-scraping-data-pipeline
description: Menyimpan, memvalidasi, dan membersihkan data mentah (raw data) hasil web scraping menggunakan standar JSON Lines (.jsonl), metadata pelacakan, serta alur pembersihan ke CSV atau Excel. Gunakan saat pengguna merancang format penyimpanan data scraping, menyusun skema metadata raw data, menulis skrip penyimpanan JSONL tahan gagal, atau membersihkan dan mengekspor hasil scraping ke CSV atau Excel.
---
# Web Scraping Data Pipeline

Skill ini menyediakan standar struktur penyimpanan data mentah (*raw data*) hasil *web scraping* beserta alur pembersihan dan ekspor ke format tabular siap analisis.

## Kapan Digunakan

- Menentukan format penyimpanan data mentah hasil *web scraping* (`.jsonl`, `.json`, `.csv`, `.html`, atau `.parquet`).
- Merancang skema objek data mentah yang memisahkan metadata teknis dari muatan hasil ekstraksi.
- Menulis skrip Python untuk menyimpan hasil *scraping* secara bertahap (*streaming/append*) agar tahan terhadap gangguan koneksi atau batas permintaan (*rate limit*).
- Membaca, memvalidasi, membersihkan, menghapus duplikasi, dan mengekspor file `.jsonl` ke format CSV atau Excel (`.xlsx`).

## Langkah Kerja

### 1. Pilih Format Penyimpanan Raw Data

Tentukan format penyimpanan berdasarkan bentuk data dan skala volume sebelum menulis skrip ekstraksi:

- **JSON Lines (`.jsonl` / `.ndjson`) - Standar utama:** Gunakan untuk skala kecil hingga besar dengan struktur semi-terstruktur atau bersarang (*nested*). Satu baris berisi satu objek JSON mandiri sehingga mendukung penulisan bertahap tanpa membebani RAM.
- **JSON (`.json`):** Gunakan hanya untuk kumpulan data kecil yang seluruhnya muat di memori sekaligus.
- **CSV / TSV (`.csv`, `.tsv`):** Gunakan hanya jika struktur kolom seragam dan datar (*flat*) tanpa teks panjang yang mengandung banyak karakter koma atau ganti baris.
- **Raw HTML / XML (`.html`, `.xml`):** Simpan respons mentah terlebih dahulu apabila aturan *parsing* (XPath/CSS selector) masih sering berubah agar ekstraksi ulang dapat dilakukan secara lokal.
- **Apache Parquet (`.parquet`):** Gunakan untuk skala jutaan baris yang langsung diproses dengan mesin analitik kolumnar (Pandas, Polars, DuckDB, Spark).

### 2. Pisahkan Metadata Teknis dan Muatan Mentah

Pertahankan sifat *immutable* pada data mentah dengan menyimpan nilai asli apa adanya tanpa pembersihan dini. Pisahkan setiap rekaman ke dalam tiga bagian utama:

- `record_id`: Pengenal unik tiap baris (misalnya UUID v4).
- `metadata`: Konteks teknis pengambilan (`source_url`, `scraped_at` dalam format ISO 8601 beserta zona waktu, `http_status`, dan `scraper_version`).
- `raw_payload`: Kamus (*dictionary*) berisi nilai mentah hasil ekstraksi halaman.

Contoh struktur satu baris pada file `.jsonl`:

```json
{"record_id": "550e8400-e29b-41d4-a716-446655440000", "metadata": {"source_url": "https://example.com/item/123", "scraped_at": "2026-01-15T08:00:00+07:00", "http_status": 200, "scraper_version": "v1.0"}, "raw_payload": {"title_raw": "  Nama Produk Contoh  ", "price_raw": "Rp 250.000", "stock_raw": "Tersisa 5 unit"}}
```

### 3. Simpan Data Secara Streaming dengan Python

Gunakan mode *append* (`"a"`) dan pengkodean `utf-8` agar setiap rekaman langsung tertulis ke disk begitu selesai diambil:

```python
import json
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

WIB = timezone(timedelta(hours=7))


def append_jsonl_record(
    file_path: str | Path,
    source_url: str,
    raw_payload: dict[str, Any],
    http_status: int = 200,
    scraper_version: str = "v1.0",
) -> None:
    """Menambahkan satu rekaman hasil scraping ke file .jsonl secara bertahap."""
    path = Path(file_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    record = {
        "record_id": str(uuid.uuid4()),
        "metadata": {
            "source_url": source_url,
            "scraped_at": datetime.now(WIB).isoformat(timespec="seconds"),
            "http_status": http_status,
            "scraper_version": scraper_version,
        },
        "raw_payload": raw_payload,
    }

    with path.open(mode="a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
```

Untuk volume ratusan ribu baris ke atas, ganti `path.open(...)` dengan `gzip.open("output.jsonl.gz", mode="at", encoding="utf-8")` guna menghemat ruang penyimpanan.

### 4. Bersihkan, Validasi, dan Ekspor ke CSV atau Excel

Pisahkan tahap pembersihan (*data cleaning*) ke skrip terpisah agar file mentah tetap utuh untuk keperluan audit:

1. **Validasi per baris:** Bungkus `json.loads()` dalam blok `try...except json.JSONDecodeError` sehingga baris yang terpotong akibat skrip berhenti mendadak hanya dilewati tanpa menggagalkan seluruh proses.
2. **Normalisasi teks dan angka:** Rapikan spasi ganda serta ganti baris dengan ekspresi reguler (`re.sub(r"\s+", " ", text).strip()`), dan ekstrak digit numerik untuk kolom harga atau jumlah.
3. **Pertahankan nilai kosong sebagai `None`:** Jangan mengubah nilai yang gagal diekstrak menjadi `0` karena akan mendistorsi perhitungan rata-rata dan distribusi data.
4. **Deduplikasi berbasis sumber:** Urutkan berdasarkan `scraped_at` menurun lalu hapus duplikasi berdasarkan `source_url` dengan mempertahankan rekaman terbaru.
5. **Ekspor ramah spreadsheet:** Gunakan `encoding="utf-8-sig"` saat menulis CSV agar Microsoft Excel di Windows mengenali karakter UTF-8 secara otomatis, serta sertakan kolom mentah pendamping bila diperlukan untuk audit manual.

```python
import json
import re
from pathlib import Path
from typing import Any
import pandas as pd


def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    cleaned = re.sub(r"\s+", " ", str(value)).strip()
    return cleaned if cleaned else None


def clean_digits(value: Any) -> int | None:
    if value is None:
        return None
    digits = re.sub(r"[^\d]", "", str(value))
    return int(digits) if digits else None


def load_jsonl_to_dataframe(input_path: str | Path) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    with Path(input_path).open(mode="r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as err:
                print(f"[Peringatan] Baris {line_num} dilewati: {err}")
                continue

            meta = record.get("metadata", {})
            payload = record.get("raw_payload", {})
            rows.append(
                {
                    "record_id": record.get("record_id"),
                    "source_url": meta.get("source_url"),
                    "scraped_at": meta.get("scraped_at"),
                    "http_status": meta.get("http_status"),
                    "title": clean_text(payload.get("title_raw")),
                    "price": clean_digits(payload.get("price_raw")),
                    "price_raw": payload.get("price_raw"),
                }
            )

    df = pd.DataFrame(rows)
    if not df.empty and "source_url" in df.columns:
        df = (
            df.sort_values(by="scraped_at", ascending=False)
            .drop_duplicates(subset=["source_url"], keep="first")
            .reset_index(drop=True)
        )
    return df


def export_tabular(df: pd.DataFrame, csv_path: str | Path, xlsx_path: str | Path) -> None:
    Path(csv_path).parent.mkdir(parents=True, exist_ok=True)
    Path(xlsx_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    df.to_excel(xlsx_path, index=False, sheet_name="Cleaned_Data")
```

## Hal yang Perlu Dihindari

- **Menggunakan `indent` pada `json.dumps()` saat menulis `.jsonl`:** Penambahan indentasi memecah satu objek menjadi banyak baris dan merusak pembacaan baris demi baris.
- **Menggunakan `ensure_ascii=True` (bawaan Python):** Mengubah karakter non-ASCII menjadi escape sequence `\uXXXX` yang memperbesar ukuran file dan menyulitkan pembacaan manual.
- **Menimpa file mentah saat pembersihan:** Selalu arahkan output pembersihan ke direktori terpisah (misalnya `data/processed/`) agar file di `data/raw/` tetap menjadi sumber kebenaran asli.
- **Mengisi angka yang hilang dengan `0`:** Membedakan nilai kosong (`None`/`NaN`) dengan nilai nol aktual mencegah kesalahan statistik saat agregasi.