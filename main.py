from ingest import ingest_data, extract_pdf, extract_csv, extract_txt, pdf_to_stg, csv_to_stg, txt_to_stg
print("Starting data ingestion...")
ingest_data() 
print("Data ingestion completed.")

print("Starting data extraction...")
extract_pdf()
extract_csv()
extract_txt()
print("Data extraction completed.")
# This is the main entry point for the data ingestion and extraction process.


print("Starting load staging...")
pdf_to_stg()
csv_to_stg()
txt_to_stg()
print("Load staging completed.")

# drop_table()