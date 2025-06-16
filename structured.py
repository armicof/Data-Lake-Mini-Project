from config import get_connection
import psycopg2
import nltk
from nltk.corpus import stopwords
from collections import Counter
import string
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
        """,

        # Dim Time
        """
        INSERT INTO dim_time (tahun, dekade)
        SELECT DISTINCT
            tahun_komersil AS tahun,
            LEFT(tahun_komersil::text, 3) || '0-an' AS dekade
        FROM financial_statement fs
        WHERE tahun_komersil IS NOT NULL
        """,

        # Dim Currency
        """
        INSERT INTO dim_currency (satuan, mata_uang)
        SELECT DISTINCT
            satuan,
            mata_uang
        FROM financial_statement fs
        WHERE satuan IS NOT NULL AND mata_uang IS NOT NULL
        """,
    ]

    conn = psycopg2.connect(get_connection())
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
            WHERE ff.company_id = dc.company_id
            AND ff.time_id = dt.time_id
            AND ff.currency_id = dcu.currency_id
        );
        """
    )
    conn.commit()
    cursor.close()
    conn.close()    

def create_txt_star_schema():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT tweet FROM tweet_data")
    tweets = cursor.fetchall()

    # Stopwords Bahasa Inggris & Indonesia
    stop_words = set(stopwords.words('english')).union(set(stopwords.words('indonesian')))

    for tweet in tweets:
        # Preprocessing
        text = tweet.lower().translate(str.maketrans('', '', string.punctuation))
        words = nltk.word_tokenize(text)
        
        # Filter kata penting
        keywords = [word for word in words if word.isalpha() and word not in stop_words]

        # Hitung frekuensi keyword
        word_freq = Counter(keywords)

        # Masukkan ke dim_keyword
        for keyword, freq in word_freq.items():
            cursor.execute("""
                INSERT INTO dim_keyword (keyword, frequency)
                VALUES (%s, %s)
            """, (keyword, freq))

    conn.commit()
    cursor.close()
    conn.close()