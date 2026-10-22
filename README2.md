# YT_ELT

## Run on a new machine

Requires Docker Desktop (or Docker Engine with Compose v2) and Git.

1. Clone the repository and enter it:
   ```bash
   git clone https://github.com/JahadNoah/YT_ETL.git
   cd YT_ETL
   ```
2. Create `.env` from the template and fill in every blank value:
   ```bash
   cp .env.example .env
   ```
   - `API_KEY`: your YouTube Data API v3 key.
   - Database and Airflow passwords: any strong values.
   - `FERNET_KEY`: generate one with
     `python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`.
   - `AIRFLOW_UID`: on Linux, set it to the output of `id -u`.
3. Make sure host port 5432 is free (`lsof -nP -iTCP:5432 -sTCP:LISTEN` on macOS/Linux). If another Postgres is using it, stop it or change the `postgres` port mapping in `docker-compose.yaml` (for example `"5433:5432"`).
4. Start the stack:
   ```bash
   docker compose up -d
   ```
5. Open the Airflow UI at http://localhost:8080 and log in with `AIRFLOW_WWW_USER_USERNAME` / `AIRFLOW_WWW_USER_PASSWORD` from `.env`.

The Postgres init script only runs when the database volume is empty. If you change database names, users or passwords in `.env` after the first start, recreate the volume with `docker compose down -v` (this deletes all data in it) and start again.

## DAGs

| DAG | Schedule (Asia/Kuala_Lumpur) | What it does |
|---|---|---|
| `produce_json` | 14:00 daily | Pulls video stats from the YouTube API into `data/YT_data_<date>.json` |
| `update_db` | 15:00 daily | Loads the JSON file into `staging.yt_api`, then transforms it into `core.yt_api` |
| `data_quality` | 16:00 daily | Runs Soda checks from `include/soda/checks.yml` on both schemas |

New DAGs start paused. Unpause them in the UI to run on schedule.

To run the Soda checks by hand inside a container (`docker exec -it airflow-worker bash`):

```bash
soda scan -d pg_datasource -c /opt/airflow/include/soda/configuration.yml -v SCHEMA=core /opt/airflow/include/soda/checks.yml
```
