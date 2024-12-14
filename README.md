# Group6TurnoverData
**Tools used in this project**
- docker
- astro cli
- apache airflow
- duckDB
- dataGrip

We are using as data sources open data two tables from Estonian Business Register and quarterly data tables from Estonian Tax and Customs Board.

**How to get started:**
- **Memory for Docker**: The default memory allocated to Docker on macOS may not be sufficient. If there isn't enough memory, the web server may continuously restart and won't operate stably. It is recommended to allocate at least 8GB of memory to Docker Engine to avoid issues.
- **To see how much memory is available, run this command**: This command checks how much physical memory is available on the system.
```bash
docker run --rm "debian:bookworm-slim" bash -c 'numfmt --to iec $(echo $(($(getconf _PHYS_PAGES) * $(getconf PAGE_SIZE))))'
```
**Mac**
```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```
```bash
brew install astro
```

**Windows**
```bash
winget install -e --id Astronomer.Astro
```

**To start airflow:**
```bash
astro dev start
```
The command should have started airflow in a docker container you can access it here: http://localhost:8080
- username and password: admin

**For the pipeline to work: Admin -> Conncetions -> Add a new record**
- Connection Id: my_local_duckdb_conn
- connection type: DuckDB
- save

**Running DAGs**
- DAGs are scheduled to run on the first day of every third month. However, the DAGs can be run from Airflow UI 
- manually here: http://localhost:8080
- Select ‘DAGs’ from the menu at the top.
- You can run the following two DAGs in parallel: "business_register_files" and "tax_and_customs_board_files"
- Don't run the third DAG "create_fact_and_dim_tables" before the first two DAGs are finished.

**Result**

- The end result is a duckDB database in the include folder
- You can access it with any database tool like dBeaver.
- For answering the business questions three sql queries were created. We used dataGrip to run the following queries:
For creating business sector turnover trend data for business sector defined by emtak code given by consultat. Answer is sector turnovers for years 2019 - 2023. To get how total turnover of software development companies is behaving use emtak=62011, for webhosting and dataprocessing business use emtak=63111. codes are given in EMTAK2008 webpage.
```sql 
SELECT f.EMTAK, f.Aruandeaasta, SUM(f.Jaotatud_myygitulu)
FROM faktitabel f
GROUP BY f.Aruandeaasta, f.EMTAK
ORDER BY EMTAK, Aruandeaasta
```
For crreating list of TOP10 companies by turnovers in business sector defined by emtak.
```sql 
SELECT f.Registikood, c.Nimi, f.Jaotatud_myygitulu, f.Aruandeaasta AS aasta
FROM faktitabel f
         JOIN company c on f.Registikood = c.Registrikood
WHERE Aruandeaasta = ?
ORDER BY Jaotatud_myygitulu DESC LIMIT 10
```
For creating conversion coefficient to compare annual report turnovers with tax autohority turnovers as turnover creation metodology is different. Tax autohority turnover data is refresed quarterly 10-days after end of quarter, but annual report turnover data is refreshed yearly with 6-months publication delay. Therefore there is interest to get more up to date data, but as methodologies differ then it is hard to compare datas. Our solution is precalculated coeficient based on last two annual reports (currently 2022-2023 data). This coeficient also hints how much company turnover is related with buying goods from other EU countries.
```sql 
SELECT c.Nimi, c.Registrikood, SUM(emta_käive) / SUM(Jaotatud_myygitulu) as konversioonikoefitsent
FROM faktitabel f
         join company c on c.Registrikood = f.Registikood
WHERE f.Aruandeaasta IN (2022, 2023)
  AND c.Registrikood = ?
GROUP BY c.Nimi, c.Registrikood
ORDER BY c.Nimi, c.Registrikood
```
