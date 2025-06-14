import os
import tabula
import pandas as pd
import shutil

dfs = tabula.read_pdf("Raw/pdf/FinancialStatement-2025-I-AADI.pdf", pages='1-2', multiple_tables=True) 
print(len(dfs))
print("Table 1:")  
print(dfs[0])
print("Table 2:")
print(dfs[1])
print("Table 3:")
print(dfs[2])

import pandas as pd
import numpy as np

# Ambil tabel kedua dari hasil tabula. Gunakan .copy() untuk menghindari warning.
df = dfs[1].copy()

# Definisikan nama kolom yang benar dan bersih
columns = ['No', 'Nama', 'Kegiatan Usaha', 'Lokasi', 'Tahun Komersil', 'Status Operasi', 'Jumlah Aset', 'Satuan', 'Mata Uang', 'Persentase (%)']

# PENTING: Pastikan jumlah kolom cocok sebelum me-rename
if len(df.columns) == len(columns):
    # Langsung ganti nama kolom dari DataFrame yang ada
    df.columns = columns
else:
    print("Peringatan: Jumlah kolom dari Tabula tidak cocok dengan jumlah kolom yang didefinisikan.")
    # Anda bisa print df.columns di sini untuk melihat nama asli dari tabula
    print("Nama kolom asli dari Tabula:", df.columns)

print("--- DataFrame Setelah Rename Kolom ---")
print(df.head()) # Tampilkan beberapa baris pertama untuk diperiksa

print("--- DataFrame Awal (Berantakan) ---")
print(df)


# LANGKAH 1: Isi nilai NaN di kolom 'No' dengan nilai valid terakhir di atasnya (Forward Fill)
df['No'] = df['No'].ffill()


# LANGKAH 2: Definisikan aturan agregasi
# Untuk kolom teks yang terpisah, kita gabungkan dengan spasi.
# Untuk kolom lain yang nilainya sudah benar di baris pertama, kita ambil nilai pertama ('first').
agg_rules = {
    'Nama': lambda x: ' '.join(x.dropna().astype(str)),
    'Kegiatan Usaha': lambda x: ' '.join(x.dropna().astype(str)),
    'Lokasi': 'first',
    'Tahun Komersil': 'first',
    'Status Operasi': 'first',
    'Jumlah Aset': 'first',
    'Satuan': 'first',
    'Mata Uang': 'first',
    'Persentase (%)': 'first'
}

# LANGKAH 3: Kelompokkan berdasarkan 'No' dan terapkan aturan agregasi
df_cleaned = df.groupby('No').agg(agg_rules).reset_index()

df

print("\n\n--- DataFrame Akhir (Setelah Diperbaiki) ---")
print(df_cleaned)