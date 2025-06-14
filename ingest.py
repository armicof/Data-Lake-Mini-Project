import os
import jpype
import shutil
import pandas as pd
import tabula
from tabula import read_pdf
import psycopg2
import csv
from config import get_connection
import re
def ingest_data():
    for filename in os.listdir('.'):
        if filename.endswith('.txt'):
            shutil.move(filename, 'Raw/txt')
        elif filename.endswith('.csv'):
            shutil.move(filename, 'Raw/csv')
        elif filename.endswith('.pdf'):
            shutil.move(filename, 'Raw/pdf')

def clean_text_column(col):
    return col.astype(str).str.replace(r'[\r\n\t]+', ' ', regex=True).str.strip()

def extract_pdf():
    for filename in os.listdir('Raw/pdf'):
        dfs = tabula.read_pdf(os.path.join('Raw/pdf', filename), pages='1-2')   
        if dfs:
            columns = ['No', 'Nama', 'Kegiatan Usaha', 'Lokasi', 'Tahun Komersil', 'Status Operasi', 'Jumlah Aset', 'Satuan', 'Mata Uang', 'Persentase (%)']
            dfs[1].columns = columns
            dfs[1]['No'] = dfs[1]['No'].ffill()  # Forward fill 'No' column
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
            dfs[1] = dfs[1].groupby('No').agg(agg_rules).reset_index()
            dfs[1]['Kegiatan Usaha'] = dfs[1]['Kegiatan Usaha'].str.replace(' ', '').str.strip()
            dfs[1]['Jumlah Aset'] = dfs[1]['Jumlah Aset'].str.replace('.', '')
            dfs[2]['Jumlah Aset'] = dfs[2]['Jumlah Aset'].str.replace('.', '')
            dfs[2].columns = columns
            df = pd.concat([dfs[1].iloc[1:],dfs[2]], ignore_index=True, join="outer")
            df = df.set_index('No', drop=True)
            df = df.reset_index(drop=True)
            for col in ['Nama', 'Kegiatan Usaha', 'Lokasi', 'Status Operasi']:
                df[col] = clean_text_column(df[col])
            df.to_csv(os.path.join('structured/pdf', filename.replace('.pdf', '.csv')), index=False)

def extract_csv():
    for filename in os.listdir('Raw/csv'):
        dfs = pd.read_csv(os.path.join('Raw/csv', filename))
        dfs["id"] = dfs["id"].astype(str).str.strip()
        dfs = dfs.set_index('id', drop=False)
        dfs['noted_date'] = pd.to_datetime(dfs['noted_date'], format="%d-%m-%Y %H:%M", errors='coerce')
        dfs.to_csv(os.path.join('structured/csv', filename), index=False)

def extract_txt():
    for filename in os.listdir('Raw/txt'):
        with open(os.path.join('structured/txt', filename.replace('.txt','.csv')), mode='w', newline='', encoding='utf-8') as csv_file:
            # Membuat object writer untuk menulis ke file CSV
            csv_writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

            # Menulis baris header
            csv_writer.writerow(['Date', 'Username', 'Tweet'])

            # Membuka file input untuk dibaca
            with open(os.path.join('Raw/txt',filename), mode='r', encoding='utf-8') as txt_file:
                # Membaca file baris per baris
                for line in txt_file:
                    # Lewati baris kosong
                    if not line.strip():
                        continue
                    
                    # Memisahkan data berdasarkan delimiter '|'
                    parts = line.split('|')
                    
                    # Pastikan baris memiliki 3 bagian agar tidak error
                    if len(parts) == 3:
                        # Bagian 1: Tanggal
                        raw_timestamp = parts[0].strip() # -> '[2025-06-13 19:45:02]'
                        # Menghilangkan kurung siku dan mengambil bagian tanggalnya saja
                        date_str = raw_timestamp[1:-1].split(' ')[0] # -> '2025-06-13'
                        
                        # Bagian 2: Username
                        username = parts[1]
                        username = re.sub(r'[^a-zA-Z0-9_]', ' ', username)
                        
                        # Bagian 3: Tweet
                        tweet = parts[2]
                        tweet = re.sub(r'[^a-zA-Z0-9]', ' ', tweet)
                        
                        # Menulis baris data yang sudah bersih ke file CSV
                        csv_writer.writerow([date_str, username, tweet])

def safe_insert(cursor, query, row):
    clean_row = [None if pd.isna(val) else val for val in row]
    cursor.execute(query, tuple(clean_row))

def pdf_to_stg():
    conn = psycopg2.connect(get_connection())
    cursor = conn.cursor()  
    # --- Structured PDF (Financial Statement)
    for filename in os.listdir('structured/pdf'):
        if filename.endswith('.csv'):
            df = pd.read_csv(os.path.join('structured/pdf', filename))
            df.columns = df.columns.str.strip()  # Hilangkan spasi
            # if 'Jumlah Aset' in df.columns:
            #     df['Jumlah Aset'] = pd.to_numeric(df['Jumlah Aset'], errors='coerce')
            for _, row in df.iterrows():
                safe_insert(cursor, """
                    INSERT INTO financial_statement (
                        nama, kegiatan_usaha, lokasi, tahun_komersil,
                        status_operasi, jumlah_aset, satuan, mata_uang, persentase
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, row)
        conn.commit()
        cursor.close()
        conn.close()    

def csv_to_stg():
    conn = psycopg2.connect(get_connection())
    cursor = conn.cursor()
    # --- Structured CSV (Temperature)
    for filename in os.listdir('structured/csv'):
        if filename.endswith('.csv'):
            df = pd.read_csv(os.path.join('structured/csv', filename))
            for _, row in df.iterrows():
                safe_insert(cursor, """
                    INSERT INTO warehouse_temperature (
                        id, room_id, noted_date, temp, in_out
                    ) VALUES (%s, %s, %s, %s, %s)
                """, row)
        conn.commit()
        cursor.close()
        conn.close()


def txt_to_stg():
    conn = psycopg2.connect(get_connection())
    cursor = conn.cursor()
    # --- Structured TXT (Tweets)
    for filename in os.listdir('structured/txt'):
        if filename.endswith('.csv'):
            df = pd.read_csv(os.path.join('structured/txt', filename))
            for _, row in df.iterrows():
                safe_insert(cursor, """
                    INSERT INTO tweet_data (
                        date, username, tweet
                    ) VALUES (%s, %s, %s)
                """, row)

    conn.commit()
    cursor.close()
    conn.close()

    