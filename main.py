from ingest import ingest_data
from analyze import pdf_to_stg, csv_to_stg, txt_to_stg, extract_pdf, extract_csv,extract_txt
from structured import create_pdf_star_schema, create_txt_star_schema, create_csv_star_schema
from load import etl_append_staging_to_dw

# print("Starting data ingestion...")
ingest_data() 
# print("Data ingestion completed.")

# print("Starting data extraction...")
extract_pdf()
extract_csv()
extract_txt()
# print("Data extraction completed.")

# print("Starting load staging...")
pdf_to_stg()
csv_to_stg()
txt_to_stg()
# print("Load staging completed.")

# print("Creating Star Schema")
create_pdf_star_schema()
create_txt_star_schema()
create_csv_star_schema()
# print("Star Schema creation completed.")

# print("Loading data to Data Warehouse")
etl_append_staging_to_dw()