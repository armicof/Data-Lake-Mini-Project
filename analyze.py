import psycopg2
import os
import pandas as pd
from config import get_connection

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
                    status_operasi, jumlah_aset, satuan, mata_uang, persentase)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (nama, kegiatan_usaha, lokasi, tahun_komersil, status_operasi, jumlah_aset, satuan, mata_uang, persentase)
                    DO UPDATE SET
                        nama = EXCLUDED.nama,
                        kegiatan_usaha = EXCLUDED.kegiatan_usaha,
                        lokasi = EXCLUDED.lokasi,
                        tahun_komersil = EXCLUDED.tahun_komersil,
                        status_operasi = EXCLUDED.status_operasi,
                        jumlah_aset = EXCLUDED.jumlah_aset,
                        satuan = EXCLUDED.satuan,
                        mata_uang = EXCLUDED.mata_uang,
                        persentase = EXCLUDED.persentase
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
                    ON CONFLICT (id, room_id, noted_date, temp, in_out)
                    DO UPDATE SET
                        id = EXCLUDED.id,
                        room_id = EXCLUDED.room_id,
                        noted_date = EXCLUDED.noted_date,
                        temp = EXCLUDED.temp,
                        in_out = EXCLUDED.in_out
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
                    ON CONFLICT (date, username, tweet)
                    DO UPDATE SET
                        date = EXCLUDED.date,
                        username = EXCLUDED.username,
                        tweet = EXCLUDED.tweet
                """, row)

    conn.commit()
    cursor.close()
    conn.close()