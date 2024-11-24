# Group6TurnoverData

**Selgitus:**
- **Mälu Dockerile**: Vaikimisi määratud mälu Dockerile macOS operatsioonisüsteemis ei pruugi olla piisav. Kui mälu pole piisavalt, võib veebiserver pidevalt taaskäivituda ja see ei tööta stabiilselt. Soovitatav on määrata vähemalt 8GB mälu Docker Engine'ile, et vältida probleeme.
- **Selleks, et näha palju mälu, käivita see käsk**: Antud käsk kontrollib, kui palju füüsilist mälu on süsteemis saadaval.
```bash
docker run --rm "debian:bookworm-slim" bash -c 'numfmt --to iec $(echo $(($(getconf _PHYS_PAGES) * $(getconf PAGE_SIZE))))'
```
- **Airflow käivitamiseks:**
```bash
astro dev start
```
- **Peaks käivituma airflow, mida näeb http://localhost:8080**
- kasutajanimi ja parool: admin

**Seejärel selleks et andmed salvestuksid: Admin -> Conncetions -> Add a new record**
- Connection Id: my_local_duckdb_conn
- connection type: DuckDB
- save
