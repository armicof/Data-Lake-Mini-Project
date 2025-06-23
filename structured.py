from config import get_stg_connection
import psycopg2
import nltk
from nltk.corpus import stopwords
from collections import Counter
import string
import re
def create_pdf_star_schema():
    dim_query = [
        # Dim Company
        """
        INSERT INTO dim_company (nama, kegiatan_usaha, lokasi, tahun_komersil, status_operasi)
        SELECT DISTINCT
            nama,
            kegiatan_usaha,
            lokasi,
            tahun_komersil,
            status_operasi
        FROM financial_statement fs
        WHERE nama IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM dim_company dc
            WHERE fs.nama = dc.nama)
        """,

        # Dim Time
        """
        INSERT INTO dim_time (tahun, dekade)
        SELECT DISTINCT
            tahun_komersil AS tahun,
            LEFT(tahun_komersil::text, 3) || '0-an' AS dekade
        FROM financial_statement fs
        WHERE tahun_komersil IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM dim_time dt
            WHERE fs.tahun_komersil = dt.tahun)
        """,

        # Dim Currency
        """
        INSERT INTO dim_currency (satuan, mata_uang)
        SELECT DISTINCT
            satuan,
            mata_uang
        FROM financial_statement fs
        WHERE satuan IS NOT NULL AND mata_uang IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM dim_currency dc
            WHERE dc.mata_uang = fs.mata_uang
            AND dc.satuan = fs.satuan)
        """,
    ]

    conn = psycopg2.connect(get_stg_connection())
    cursor = conn.cursor()
    
    for q in dim_query:
        cursor.execute(q)
    conn.commit()

    cursor.execute(
        """
        INSERT INTO financial_fact (company_id, time_id, currency_id, jumlah_aset, persentase_kepemilikan)
        SELECT
            dc.company_id,
            dt.time_id,
            dcu.currency_id,
            fs.jumlah_aset,
            fs.persentase AS persentase_kepemilikan
        FROM financial_statement fs
        JOIN dim_company dc ON dc.nama = fs.nama
        JOIN dim_time dt ON dt.tahun = fs.tahun_komersil
        JOIN dim_currency dcu ON dcu.satuan = fs.satuan AND dcu.mata_uang = fs.mata_uang
        WHERE fs.nama IS NOT NULL AND fs.tahun_komersil IS NOT NULL AND fs.satuan IS NOT NULL AND fs.mata_uang IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM financial_fact ff
            WHERE ff.time_id = dt.time_id
            AND ff.currency_id = dcu.currency_id
            AND ff.jumlah_aset = fs.jumlah_aset
            AND ff.persentase_kepemilikan = fs.persentase
        );
        """
    )
    conn.commit()
    cursor.close()
    conn.close()    

def create_txt_star_schema():
    conn = psycopg2.connect(get_stg_connection())
    cursor = conn.cursor()

    dim_date_query ="""
        INSERT INTO dim_date (date, year, month, day)
        SELECT DISTINCT
            date,
            EXTRACT(YEAR FROM date),
            EXTRACT(MONTH FROM date),
            EXTRACT(DAY FROM date)
        FROM tweet_data td
        WHERE date IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM dim_date dt
            WHERE dt.date = td.date);
        """
    cursor.execute(dim_date_query)
    cursor.execute("SELECT tweet FROM tweet_data")
    tweets = cursor.fetchall()

    # Stopwords Bahasa Inggris & Indonesia
    stop_words = set(stopwords.words('english')).union(set(stopwords.words('indonesian')))

    for tweet in tweets:
        # Ambil string dari tuple
        text = tweet[0].lower().translate(str.maketrans('', '', string.punctuation))
        words = nltk.word_tokenize(text)
        
        # Filter kata penting
        keywords = [word for word in words if word.isalpha() and word not in stop_words]

        # Hitung frekuensi keyword
        word_freq = Counter(keywords)

        # Masukkan ke dim_keyword
        for keyword, freq in word_freq.items():
            cursor.execute("""
                INSERT INTO dim_word (word, freq)
                VALUES (%s, %s)
                ON CONFLICT (word) DO NOTHING
            """, (keyword, freq))


    cursor.execute("""
        INSERT INTO dim_user (username)
        SELECT DISTINCT
            username 
        FROM tweet_data td
        WHERE username IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM dim_user du
            WHERE td.username = du.username);
    """)
    conn.commit()
    cursor.execute("""
        CREATE TEMP TABLE temp_word_freq AS
        SELECT
            td.id AS tweet_id,
            td.username,
            td.date,
            keyword,
            COUNT(*) AS freq
        FROM tweet_data td,
            unnest(string_to_array(lower(td.tweet), ' ')) AS keyword
        WHERE keyword ~ '^[a-z]+$'
        AND td.id NOT IN (
            SELECT DISTINCT tweet_id FROM fact_wordcloud WHERE tweet_id IS NOT NULL
        )
        GROUP BY td.id, td.username, td.date, keyword;
    """)
        
    cursor.execute("""
        INSERT INTO fact_wordcloud (tweet_id, word_id, time_id, user_id, frequency)
        SELECT
            tf.tweet_id,
            dw.word_id,
            dt.time_id,
            du.user_id,
            tf.freq
        FROM temp_word_freq tf
        JOIN dim_word dw ON tf.keyword = dw.word
        JOIN dim_date dt ON tf.date = dt.date
        JOIN dim_user du ON tf.username = du.username;
    """)
    conn.commit()
    cursor.close()
    conn.close()

def create_csv_star_schema():
    conn = psycopg2.connect(get_stg_connection())
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO dim_times (full_date, day, month, year, hour, minute)
        SELECT DISTINCT
            noted_date,
            EXTRACT(DAY FROM noted_date),
            EXTRACT(MONTH FROM noted_date),
            EXTRACT(YEAR FROM noted_date),
            EXTRACT(HOUR FROM noted_date),
            EXTRACT(MINUTE FROM noted_date)
        FROM warehouse_temperature
        WHERE noted_date IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM dim_times dt
            WHERE dt.full_date = warehouse_temperature.noted_date
        );
    """)
    
    cursor.execute("""
        INSERT INTO dim_room (room_name)
        SELECT DISTINCT room_id FROM warehouse_temperature
        WHERE room_id IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM dim_room dr
            WHERE dr.room_name = warehouse_temperature.room_id
        );
    """)

    cursor.execute("""
        INSERT INTO fact_room_activities (room_id, time_id, in_out, temperature)
        SELECT 
            r.room_id,
            t.time_id,
            d.in_out,
            d.temp
        FROM warehouse_temperature d
        JOIN dim_room r ON d.room_id = r.room_name
        JOIN dim_times t ON d.noted_date = t.full_date
        WHERE d.room_id IS NOT NULL AND d.noted_date IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM fact_room_activities fra
            WHERE fra.room_id = r.room_id
            AND fra.time_id = t.time_id
            AND fra.in_out = d.in_out
            AND fra.temperature = d.temp
        );
    """)

    conn.commit()
    cursor.close()
    conn.close()