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

#The path to the data lake where raw downloaded files are stored
data_lake_dir = os.path.abspath("dataLake")

#First DAG - downloading and processing files from Business register
@dag(start_date=datetime(2023, 6, 1), schedule=None, catchup=False)
def business_register_files():
    #First task - downloading .zip files
    @task
    def downloadfiles():
        base_url = "https://avaandmed.ariregister.rik.ee/et/avaandmete-allalaadimine"
        print(data_lake_dir)
        os.makedirs(data_lake_dir, exist_ok=True)
        #Searching for specific header to download the .zip files we need
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

    #Second task - unzip specific files. From the downloaded files, we need only two. Here we unzip
    # the files and save them to a separate folder in the data lake.
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

    #Third task - converting .csv files to Pandas dataframe. Here we are converting .csv files
    # to Pandas dataframe, including only necessary columns.
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

    #Forth task - creating DB tables. Here we load the data to the database by creating the table
    #and inserting the data.
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

    # Task Dependencies.
    download_files = downloadfiles()
    #unzip file function doesn't use the given parameter, but it was necessary to add it there
    #in order to have the tasks triggered sequentially, because we need to have files downloaded
    #before starting unzipping them.
    unzip_result = unzip_files(download_files)
    dataframes = create_pandas_df(unzip_result)
    create_duckdb_table(dataframes)

business_register_files()

#Second DAG - downloading and processing files from tax and customs board
@dag(start_date=datetime(2023, 6, 1), schedule=None, catchup=False)
def tax_and_customs_board_files():
    #First task - downloading .csv files from another source.
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

    #Second task - creating combined dataframe from .csv files. We downloaded 16 .csv files, and
    #here we combine them into a single dataframe, keeping only necessary columns.
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
                    columns_to_keep = ["Registrikood","Nimi","Kaive","Maakond"]
                    df = pd.read_csv(f, sep=";")
                    df = df[columns_to_keep]
                    #In order to convert string into float, we first need to remove unnecessary spaces
                    #and replace comme to dot.
                    df["Kaive"] = (
                        df["Kaive"]
                        .str.replace(" ", "")
                        .str.replace(",", ".")
                    )
                    #The file names of the .csv files also have the information about year and period
                    #so we split the file names and saved the information into a separate column
                    split_filename = file.split("_")
                    df["aasta"] = split_filename[2]
                    df["kvartal"] = split_filename[3]
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

    #Third task - creating a new database table. Here we are creating a new DB table and inserting
    #the data from dataframe into the database.
    @task
    def create_duckdb_table(dataframe):
        conn = duckdb.connect("include/turnover_data.db")
        conn.register("kaive_view", dataframe)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS kaive (Registrikood VARCHAR,
                Nimi VARCHAR,
                Kaive FLOAT,
                Maakond VARCHAR,
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
                CAST(Maakond AS VARCHAR) AS Maakond,
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

#Third DAG - creating combined fact tabel.
@dag(start_date=datetime(2023, 6, 1), schedule=None, catchup=False)
def create_fact_and_dim_tables():
    #First task - creating dimension table for company
    @task
    def create_company_table_dim():
        conn = duckdb.connect("include/turnover_data.db")
        conn.execute(
            """
            CREATE SEQUENCE IF NOT EXISTS id_faktitabel START 1;
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS company (Registrikood VARCHAR PRIMARY KEY,
                Nimi VARCHAR,
                Maakond VARCHAR
            )
            """
        )
        conn.execute(
            """
            SELECT DISTINCT k.Registrikood,
                (SELECT k2.Nimi
                FROM kaive k2
                WHERE k2.Registrikood = k.Registrikood LIMIT 1),
                (SELECT k2.Maakond
                FROM kaive k2
                WHERE k2.Registrikood = k.Registrikood LIMIT 1)
                FROM kaive k WHERE NOT EXISTS (SELECT 1
                FROM kaive k1 WHERE k1.Registrikood = k.Registrikood);           
            """
        )
        conn.close()

    #Second task - creating dimension table for EMTAK(The Classification of Economic Activities in Estonia)
    #with NACE (NATIONAL CLASSIFICATION OF ECONOMIC ACTIVITIES)
    @task
    def create_conversion_table_dim(company_table):
        conn = duckdb.connect("include/turnover_data.db")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS conversion (emtak VARCHAR PRIMARY KEY,
                nace VARCHAR
            )
            """
        )
        conn.execute(
            """
            INSERT INTO conversion( 
            SELECT DISTINCT
                m.emtak,
                SUBSTRING(m.emtak FROM 1 FOR 4) AS nace
            FROM myygitulu m WHERE NOT EXISTS (SELECT 1
            FROM myygitulu m1
            WHERE m1.emtak = m.emtak)                  
            )
            """
        )
        conn.close()

    # Third task - joining DB tables and creating combined fact table
    @task
    def join_tables(conversion_table):
        conn = duckdb.connect("include/turnover_data.db")
        # Faktitabel means fact table
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS faktitabel (
                ID INTEGER DEFAULT nextval('id_faktitabel') PRIMARY KEY,
                ReportId VARCHAR,
                EMTAK VARCHAR,
                Jaotatud_myygitulu FLOAT,
                Registikood VARCHAR ,
                Aruandeaasta INTEGER,
                PeriodEnd DATE,
                emta_käive FLOAT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO faktitabel (ReportId, EMTAK, Jaotatud_myygitulu, Registikood, Aruandeaasta, PeriodEnd, emta_käive)
            SELECT m.ReportId,
                   m.EMTAK,
                   m.Jaotatud_myygitulu,
                   y.Registikood,
                   y.Aruandeaasta,
                   y.PeriodEnd,
                   (SELECT SUM(k.Kaive)
                    FROM kaive k
                    WHERE k.Registrikood = y.Registikood
                    AND y.Aruandeaasta = k.Aasta
                    GROUP BY k.Registrikood, k.Aasta) as emta_käive
            FROM myygitulu m
            JOIN yldandmed y ON m.ReportId = y.ReportId;
            """
        )
        conn.close()
    #Forth task - removing unnecessary DB tables
    @task
    def remove_raw_tables(final_tables):
        conn = duckdb.connect("include/turnover_data.db")
        conn.execute(
            """
            DROP TABLE IF EXISTS myygitulu;
            """
        )
        conn.execute(
            """
            DROP TABLE IF EXISTS yldandmed;
            """
        )
        conn.execute(
            """
            DROP TABLE IF EXISTS kaive;
            """
        )
    #Dependencies
    company_table = create_company_table_dim()
    conversion_table = create_conversion_table_dim(company_table)
    final_tables = join_tables(conversion_table)
    remove_raw_tables(final_tables)
create_fact_and_dim_tables()