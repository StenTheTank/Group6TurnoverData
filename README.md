# Group6TurnoverData

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
