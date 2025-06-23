import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from config import get_stg_connection, get_dw_connection
def etl_append_staging_to_dw():
    try:
        # Connect using psycopg2
        stg_engine = create_engine(get_stg_connection())
        dw_engine = create_engine(get_dw_connection())

        # SQLAlchemy engine for writing
        dw_engine = create_engine(f'postgresql+psycopg2://{get_dw_connection().split("//")[1]}')

        # Extract data
        stg_dim_room = pd.read_sql('SELECT * FROM dim_room', stg_engine)
        stg_dim_times = pd.read_sql('SELECT * FROM dim_times', stg_engine)
        stg_fact = pd.read_sql('SELECT * FROM fact_room_activities', stg_engine)

        dw_dim_room = pd.read_sql('SELECT * FROM dim_room', dw_engine)
        dw_dim_times = pd.read_sql('SELECT * FROM dim_times', dw_engine)
        dw_fact = pd.read_sql('SELECT * FROM fact_room_activities', dw_engine)

        # Filter new data
        new_rooms = stg_dim_room[~stg_dim_room['room_id'].isin(dw_dim_room['room_id'])]
        new_times = stg_dim_times[~stg_dim_times['time_id'].isin(dw_dim_times['time_id'])]
        new_facts = stg_fact[~stg_fact['activity_id'].isin(dw_fact['activity_id'])]

        # Append to DW
        if not new_rooms.empty:
            new_rooms.to_sql('dim_room', dw_engine, if_exists='append', index=False)
        if not new_times.empty:
            new_times.to_sql('dim_times', dw_engine, if_exists='append', index=False)
        if not new_facts.empty:
            new_facts.to_sql('fact_room_activities', dw_engine, if_exists='append', index=False)

        # Read from staging
        stg_dim_user = pd.read_sql('SELECT * FROM dim_user', stg_engine)
        stg_dim_date = pd.read_sql('SELECT * FROM dim_date', stg_engine)
        stg_dim_word = pd.read_sql('SELECT * FROM dim_word', stg_engine)
        stg_fact_wordcloud = pd.read_sql('SELECT * FROM fact_wordcloud', stg_engine)

        # Read from DW
        dw_dim_user = pd.read_sql('SELECT * FROM dim_user', dw_engine)
        dw_dim_date = pd.read_sql('SELECT * FROM dim_date', dw_engine)
        dw_dim_word = pd.read_sql('SELECT * FROM dim_word', dw_engine)
        dw_fact_wordcloud = pd.read_sql('SELECT * FROM fact_wordcloud', dw_engine)

        # Compare and find new data
        new_users = stg_dim_user[~stg_dim_user['user_id'].isin(dw_dim_user['user_id'])]
        new_dates = stg_dim_date[~stg_dim_date['time_id'].isin(dw_dim_date['time_id'])]
        new_words = stg_dim_word[~stg_dim_word['word_id'].isin(dw_dim_word['word_id'])]
        new_wordcloud_facts = stg_fact_wordcloud[~stg_fact_wordcloud['id'].isin(dw_fact_wordcloud['id'])]

        # Append new rows
        if not new_users.empty:
            new_users.to_sql('dim_user', dw_engine, if_exists='append', index=False)
        if not new_dates.empty:
            new_dates.to_sql('dim_date', dw_engine, if_exists='append', index=False)
        if not new_words.empty:
            new_words.to_sql('dim_word', dw_engine, if_exists='append', index=False)
        if not new_wordcloud_facts.empty:
            new_wordcloud_facts.to_sql('fact_wordcloud', dw_engine, if_exists='append', index=False)

        # Read from staging
        stg_dim_company = pd.read_sql('SELECT * FROM dim_company', stg_engine)
        stg_dim_currency = pd.read_sql('SELECT * FROM dim_currency', stg_engine)
        stg_dim_time = pd.read_sql('SELECT * FROM dim_time', stg_engine)
        stg_financial_fact = pd.read_sql('SELECT * FROM financial_fact', stg_engine)

        # Read from DW
        dw_dim_company = pd.read_sql('SELECT * FROM dim_company', dw_engine)
        dw_dim_currency = pd.read_sql('SELECT * FROM dim_currency', dw_engine)
        dw_dim_time = pd.read_sql('SELECT * FROM dim_time', dw_engine)
        dw_financial_fact = pd.read_sql('SELECT * FROM financial_fact', dw_engine)

        # Compare and find new data
        new_companies = stg_dim_company[~stg_dim_company['company_id'].isin(dw_dim_company['company_id'])]
        new_currencies = stg_dim_currency[~stg_dim_currency['currency_id'].isin(dw_dim_currency['currency_id'])]
        new_times_fin = stg_dim_time[~stg_dim_time['time_id'].isin(dw_dim_time['time_id'])]
        new_facts_fin = stg_financial_fact[~stg_financial_fact['id'].isin(dw_financial_fact['id'])]

        # Append new rows
        if not new_companies.empty:
            new_companies.to_sql('dim_company', dw_engine, if_exists='append', index=False)
        if not new_currencies.empty:
            new_currencies.to_sql('dim_currency', dw_engine, if_exists='append', index=False)
        if not new_times_fin.empty:
            new_times_fin.to_sql('dim_time', dw_engine, if_exists='append', index=False)
        if not new_facts_fin.empty:
            new_facts_fin.to_sql('financial_fact', dw_engine, if_exists='append', index=False)

        print("✅ Incremental ETL from staging to DW completed successfully.")

    except Exception as e:
        print("❌ ETL failed:", e)

    finally:
        if stg_engine in locals():
            stg_engine.dispose()
        if dw_engine in locals():
            dw_engine.dispose()