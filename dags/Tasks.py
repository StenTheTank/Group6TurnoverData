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

#First DAG - downloading and processing files from Business register
@dag(start_date=datetime(2023, 6, 1), schedule=None, catchup=False)
def business_register_files():
    #Fisrt task - downloading .zip files
    @task
    def downloadfiles():
        base_url = "https://avaandmed.ariregister.rik.ee/et/avaandmete-allalaadimine"
        print(data_lake_dir)
        os.makedirs(data_lake_dir, exist_ok=True)
        try:
            response = requests.get(base_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            header = soup.find("h2", string="Majandusaasta aruande andmed")
            if header:
                parent_div = header.find_next("div")
                file_links = parent_div.find_all("a", href=True)
                for link in file_links:
                    file_url = link["href"]
                    if not file_url.endswith(".zip"):
                        continue
                    full_url = urljoin(base_url, file_url)
                    file_name = os.path.basename(file_url)
                    print(f"Downloading {file_name} from {full_url}...")
                    file_response = requests.get(full_url)
                    file_response.raise_for_status()
                    with open(os.path.join(data_lake_dir, file_name), "wb") as file:
                        file.write(file_response.content)
                    print(f"Saved: {file_name}")
            else:
                print("Specified header not found on the page.")
        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")

    #Second task - unzip specific files
    @task
    def unzip_files(download_files):
        partial_names = ["EMTAK", "yldandmed"]
        os.makedirs(data_lake_dir, exist_ok=True)
        extracted_files = []
        extract_dir = os.path.join(data_lake_dir, "extracted_files")
        os.makedirs(extract_dir, exist_ok=True)
        print(extracted_files)
        for partial_name in partial_names:
            matching_zip_files = [file for file in os.listdir(data_lake_dir) if file.endswith(".zip") and partial_name in file]
            print(f"Matching files for '{partial_name}': {matching_zip_files}")
            for zip_file in matching_zip_files:
                target_zip_path = os.path.join(data_lake_dir, zip_file)
                with zipfile.ZipFile(target_zip_path, 'r') as zip_ref:
                    zip_ref.extractall(extract_dir)
                    print(f"Extracted {zip_file} to {extract_dir}")
                extracted_files.append(zip_ref.namelist()[0])
        if extracted_files:
            print("All extracted files:", extracted_files)
            return {"Extracted files": extracted_files, "dir": extract_dir}
        else:
            print(f"No ZIP files containing the specified partial names found in {data_lake_dir}.")
            return {"Extracted files": extracted_files, "dir": extract_dir}

    #Third task - converting .csv files to Pandas dataframe
    @task
    def create_pandas_df(unzip_result):
        extracted_files = unzip_result["Extracted files"]
        dir = unzip_result["dir"]
        emtak_df = pd.read_csv(os.path.join(dir, extracted_files[0]), sep=";")
        emtak_df = emtak_df.rename(columns={"Jaotatud müügitulu":"jaotatud_myygitulu"})
        yldandmed_df = pd.read_csv(os.path.join(dir, extracted_files[1]), sep=";")
        columns_to_keep = ["report_id", "registrikood", "aruandeaast", "period_end"]
        yldandmed_df["registrikood"] = yldandmed_df["registrikood"]
        yldandmed_df["period_end"] = pd.to_datetime(yldandmed_df["period_end"])
        yldandmed_df = yldandmed_df[columns_to_keep]
        return {"myygitulu": emtak_df, "yldandmed": yldandmed_df}

    #Forth task - creating DB tables
    @task
    def create_duckdb_table(dataframes):
        conn = duckdb.connect("include/turnover_data.db")
        conn.register("myygitulu_view", dataframes["myygitulu"])
        conn.register("yldandmed_view", dataframes["yldandmed"])
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS myygitulu (ReportId VARCHAR,
                EMTAK VARCHAR,
                Jaotatud_myygitulu FLOAT,
                Pohitegevus VARCHAR
            )
            """
        )
        conn.execute(
            """
            INSERT INTO myygitulu
            SELECT 
                CAST(report_id AS VARCHAR) AS ReportId,
                CAST(emtak AS VARCHAR) AS EMTAK,
                CAST(jaotatud_myygitulu AS FLOAT) AS Jaotatud_myygitulu,
                CAST(põhitegevusala AS VARCHAR) AS Pohitegevus
            FROM myygitulu_view
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS yldandmed (ReportId VARCHAR,
                Registikood VARCHAR,
                Aruandeaasta INT,
                PeriodEnd DATE
            )
            """
        )
        conn.execute(
            """
            INSERT INTO yldandmed
            SELECT 
                CAST(report_id AS VARCHAR) AS ReportId,
                CAST(registrikood AS VARCHAR) AS Registikood,
                CAST(aruandeaast AS INT) AS Aruandeaasta,
                CAST(period_end AS DATE) AS PeriodEnd
            FROM yldandmed_view
            """
        )
        top5 = conn.execute("SELECT * FROM myygitulu LIMIT 5").fetchall()
        top5_2 = conn.execute("SELECT * FROM yldandmed LIMIT 5").fetchall()
        print("Top 5 records from myygitulu:", top5)
        print("Top 5 records from yldandmed:", top5_2)
        conn.close()

    # Task Dependencies
    download_files = downloadfiles()
    unzip_result = unzip_files(download_files)
    dataframes = create_pandas_df(unzip_result)
    create_duckdb_table(dataframes)

business_register_files()

#Second DAG - downloading and processing files from tax and customs board
@dag(start_date=datetime(2023, 6, 1), schedule=None, catchup=False)
def tax_and_customs_board_files():
    #First task - downloading .csv files
    @task
    def download_csv_files():
        base_url = "https://www.emta.ee/eraklient/amet-uudised-ja-kontakt/uudised-pressiinfo-statistika/statistika-ja-avaandmed#tasutud-maksud-failid"
        response = requests.get(base_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        accordion = soup.find("div", id="tasutud-maksud-failid")
        if not accordion:
            print("Accordion with ID 'tasutud-maksud-failid' not found!")
            return
        csv_links = accordion.find_all("a", href=True)
        csv_links = [link for link in csv_links if ".csv" in link["href"]]
        print(f"Found {len(csv_links)} CSV files in 'tasutud-maksud-failid'.")
        for link in csv_links:
            file_url = urljoin(base_url, link["href"])
            file_name = os.path.basename(link["href"])
            file_path = os.path.join(data_lake_dir, file_name)
            print(f"Downloading {file_name} from {file_url}...")
            file_response = requests.get(file_url)
            file_response.raise_for_status()
            with open(file_path, "wb") as file:
                file.write(file_response.content)
                print(f"Saved: {file_path}")

    #Second task - creating combined dataframe from .csv files
    @task
    def create_pandas_df(downloaded_files):
        dataframes = []
        partial_name = 'tasutud_maksud'
        matching_csv_files = [file for file in os.listdir(data_lake_dir) if file.endswith(".csv") and partial_name in file]
        print(f"Number of matching files: {len(matching_csv_files)}")
        if not matching_csv_files:
            print("No matching files found.")
            return pd.DataFrame()
        for file in matching_csv_files:
            file_path = os.path.join(data_lake_dir, file)
            print(f"Reading file: {file_path}")
            try:
                with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                    columns_to_keep = ["Registrikood","Nimi","Kaive"]
                    df = pd.read_csv(f, sep=";")
                    df = df[columns_to_keep]
                    df["Kaive"] = (
                        df["Kaive"]
                        .str.replace(" ", "")
                        .str.replace(",", ".")
                    )
                    list = file.split("_")
                    df["aasta"] = list[2]
                    df["kvartal"] = list[3]
                    dataframes.append(df)
            except Exception as e:
                print(f"Error reading file {file}: {e}")
        if dataframes:
            combined_df = pd.concat(dataframes, ignore_index=True)
            print("Combined DataFrame preview:")
            print(combined_df.head())
            return combined_df
        else:
            print("No dataframes to combine.")
            return pd.DataFrame()

    #Third task - creating a new database table
    @task
    def create_duckdb_table(dataframe):
        conn = duckdb.connect("include/turnover_data.db")
        conn.register("kaive_view", dataframe)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS kaive (Registrikood VARCHAR,
                Nimi VARCHAR,
                Kaive FLOAT,
                Aasta INT,
                Kvartal VARCHAR
            )
            """
        )
        conn.execute(
            """
            INSERT INTO kaive
            SELECT 
                CAST(Registrikood AS VARCHAR) AS Registrikood,
                CAST(Nimi AS VARCHAR) AS Nimi,
                CAST(Kaive AS FLOAT) AS Kaive,
                CAST(aasta AS INT) AS Aasta,
                CAST(kvartal AS VARCHAR) AS Kvartal,
            FROM kaive_view
            """
        )

        top5 = conn.execute("SELECT * FROM kaive LIMIT 5").fetchall()
        describe = conn.execute("DESCRIBE kaive").fetchall()
        print("Top 5 records from kaive:", top5)
        print(describe)
        conn.close()

    #Dependencies
    downloaded_files = download_csv_files()
    dataframe = create_pandas_df(downloaded_files)
    create_duckdb_table(dataframe)

tax_and_customs_board_files()