import os
import shutil

def ingest_data():
    for filename in os.listdir('.'):
        if filename.endswith('.txt'):
            shutil.move(filename, 'Raw/txt')
        elif filename.endswith('.csv'):
            shutil.move(filename, 'Raw/csv')
        elif filename.endswith('.pdf'):
            shutil.move(filename, 'Raw/pdf')





    