# PostgreSQL Table Size Monitor (Airflow DAG)

This project provides an Apache Airflow DAG that monitors table sizes in a PostgreSQL database and logs them into a metrics database for tracking and analysis.
The DAG runs on a weekly basis, at 6 AM on Monday, and logs the size of each table in the specified schema. The schedule can be configured based on the user's needs. It also supports monitoring all tables in a schema or specific tables. These parameters are set to default values in the `monitor_pg_table_size.py` file, under the `default_params` dictionary. The default values are listed below:
   `schema_name`: 'public'
   `monitor_tables`: 'all'
These parameters can be overridden by the user when running the DAG in Airflow manually with a clear description of each parameter.


## Features
- Monitors PostgreSQL table sizes on a schedule.
- Supports:
  - Monitoring all tables in a schema.
  - Monitoring specific tables (comma-separated).
  - Monitoring specific schemas.
- Stores historical size metrics in a dedicated table `monitored_table_sizes`.
- Runs fully automated inside Airflow with configurable parameters.

## Usage
To use this project, follow these steps:
1. Clone the repository to your local machine. To avoid any issues, make sure you clone the `main` branch.
2. Open the project directory in your favorite text editor (e.g., Visual Studio Code) and run these commands:
   - `docker network create airflow-net`
   Creates a dedicated Docker network so all services (Airflow, Postgres, Redis, etc.) can communicate with each other.
   - `docker compose up airflow-init`
   Runs the one-time initialization service to set up the Airflow database, create the admin user, and prepare required connections.
   - `docker compose up`
   Starts all containers defined in docker-compose.yml (webserver, scheduler, workers, Redis, Postgres, etc.) and runs the Airflow cluster.
3. After the containers are up and running, you can access the UIs at:
   - Airflow: `http://localhost:8080` username: `airflow` password: `airflow`
   - Postgres: `http://localhost:5050` username: `admin` password: `admin`
   All service credentials are defined and can be changed in the `docker-compose.yml` file.
   All database credentials are defined and can be changed in the `.env` file.

## Next Steps
If you want to monitor your PostgresSQL database, you can follow these steps:
1. Open `localhost:8080` and login.
2. Go to `Admin` → `Connections`.
3. Edit the `target_pg_conn` connection to match your database credentials.





