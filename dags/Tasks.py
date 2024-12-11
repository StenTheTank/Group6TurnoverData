import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from airflow.decorators import dag, task
from pendulum import datetime
import duckdb
import pandas as pd
import zipfile
from duckdb_provider.hooks.duckdb_hook import DuckDBHook

DUCKDB_CONN_ID = "my_local_duckdb_conn"
DUCKDB_TABLE_NAME = "emtak_myygitulu"
data_lake_dir = os.path.abspath("dataLake")

@dag(start_date=datetime(2023, 6, 1), schedule=None, catchup=False)
def process_file_to_duckdb():
    @task
    def downloadfile():
        base_url = "https://avaandmed.ariregister.rik.ee/et/avaandmete-allalaadimine"
        print(data_lake_dir)
        os.makedirs(data_lake_dir, exist_ok=True)
        try:
            response = requests.get(base_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")

            header = soup.find("h2", string="Majandusaasta aruande andmed")

            if header:
                # Find the parent or the block containing the files below this header
                parent_div = header.find_next("div")  # Get the next div after the header

                # Extract all <a> tags within this block
                file_links = parent_div.find_all("a", href=True)

                for link in file_links:
                    file_url = link["href"]

                    # Skip if the href is not a downloadable file type
                    if not file_url.endswith(".zip"):
                        continue

                    # Create the full URL for the file
                    full_url = urljoin(base_url, file_url)

                    # Extract the file name
                    file_name = os.path.basename(file_url)

                    # Download the file
                    print(f"Downloading {file_name} from {full_url}...")
                    file_response = requests.get(full_url)
                    file_response.raise_for_status()

                    # Save the file locally
                    with open(os.path.join(data_lake_dir, file_name), "wb") as file:
                        file.write(file_response.content)

                    print(f"Saved: {file_name}")
            else:
                print("Specified header not found on the page.")
        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")

    @task
    def unzip_file():
        partial_name = "EMTAK"
        # Find the ZIP file matching the partial name
        os.makedirs(data_lake_dir, exist_ok=True)
        matching_zip_files = [file for file in os.listdir(data_lake_dir) if file.endswith(".zip") and partial_name in file]
        print(data_lake_dir)
        print(matching_zip_files)

        if matching_zip_files:
            # Select the first matching file (or handle multiple matches)
            target_zip_name = matching_zip_files[0]
            target_zip_path = os.path.join(data_lake_dir, target_zip_name)

            # Directory to extract the ZIP file
            extract_dir = os.path.join(data_lake_dir, "extracted_files")
            os.makedirs(extract_dir, exist_ok=True)

            # Unzip the specific file
            with zipfile.ZipFile(target_zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
                print(f"Extracted {target_zip_name} to {extract_dir}")

            # List extracted files
            extracted_files = os.listdir(extract_dir)
            print("Extracted files:", extracted_files)
            return {"csv_file": extracted_files[0], "dir": extract_dir}
        else:
            print(f"No ZIP file containing '{partial_name}' found in {data_lake_dir}.")

    @task
    def create_pandas_df(unzip_result):
        csv_file = unzip_result["csv_file"]
        dir = unzip_result["dir"]
        emtak_df = pd.read_csv(os.path.join(dir, csv_file), sep=";")
        return emtak_df

    @task
    def create_duckdb_table(dataframe):
        conn = duckdb.connect("include/turnover_data.db")
        conn.sql(
            """
            CREATE TABLE IF NOT EXISTS emtak_myygitulu AS
            SELECT * FROM dataframe
            """
        )
        top5 = conn.execute("SELECT * FROM emtak_myygitulu LIMIT 5").fetchall()
        print("Top 5 records from DuckDB:", top5)
        conn.close()

    # Task Dependencies
    downloadfile()
    unzip_result = unzip_file()
    dataframe = create_pandas_df(unzip_result)
    create_duckdb_table(dataframe)

process_file_to_duckdb()