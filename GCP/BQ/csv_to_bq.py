import argparse
import os
import re
import shutil
import zipfile

import pandas as pd
from google.cloud import bigquery


def unzip_csv(zip_file_path, output_folder):
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(output_folder)


def load_batch_to_bq(df, table_name, job_config, counter):
    client = bigquery.Client()

    batch_df = pd.concat([df], ignore_index=True)

    print(f"Loading chunk {counter} to BigQuery table {table_name}")
    job = client.load_table_from_dataframe(batch_df, table_name, job_config=job_config)
    job.result()


def load_data_to_bq(csv_file_path, table_name):
    client = bigquery.Client()
    counter = 0
    chunksize = 10 ** 6
    for chunk in pd.read_csv(csv_file_path, dtype=str, chunksize=chunksize):
        chunk.columns = [re.sub(r'[\(\)\s\.\[\]]', '', each) for each in list(chunk.columns)]

        # Configuring BigQuery job options
        job_config = bigquery.LoadJobConfig(
            schema=[bigquery.SchemaField(column, 'STRING')
                    for column in list(chunk.columns)],
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

        load_batch_to_bq(chunk, table_name, job_config, counter)
        counter += 1


def main(args):
    temp_folder = 'temp_csv_folder'
    os.makedirs(temp_folder, exist_ok=True)

    unzip_csv(args.zip_file, temp_folder)

    # Finding the CSV file in the extracted folder (assuming only one CSV file in the zip)
    csv_file_path = os.path.join(temp_folder, os.listdir(temp_folder)[0])

    # Loading data to BigQuery table
    load_data_to_bq(csv_file_path, args.table_name)

    # Cleanup: removing temporary folder
    shutil.rmtree(temp_folder, ignore_errors=True)

    print(f"Data has been loaded to {args.table_name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load CSV data into BigQuery table in parallel batches.")
    parser.add_argument("--zip_file", type=str, help="Path to the zip file containing CSV data.")
    parser.add_argument("--table_name", type=str, help="Name of the BigQuery table.")

    args = parser.parse_args()
    main(args)
