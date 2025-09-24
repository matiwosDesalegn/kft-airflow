

● Airflow Full Database Restore Workflow Implementation Complete! 🎉

  Your workflow has been implemented in /home/airflow/airflow/dags/full_database_restore_workflow.py with the following features:

  DAG Structure:

  - DAG ID: full_database_restore_workflow
  - Schedule: Manual trigger only (safe for restore operations)
  - Max Active Runs: 1 (prevents concurrent restores)

  Tasks Implemented:

  1. 🔎 Find Latest PostgreSQL Dump (find_latest_postgres_dump)
    - Searches S3 bucket kft-lakehouse-staging with prefix digital_lending/backups/zemzem/postgres/
    - Filters for files starting with fineract_postgres_backup_ or marketplace_postgres_backup_
    - Downloads latest .gz file to /tmp/latest_pg.gz
  2. 🔎 Find Latest MongoDB Dumps (find_latest_mongodb_dump)
    - Searches for customer-management_db and decision-db dumps
    - Downloads latest .archive files to /tmp/
  3. 💾 Full PostgreSQL Restore (restore_postgres_dump)
    - Uses pg_restore --clean for complete schema and data restore
    - Command: pg_restore --verbose --clean --no-acl --no-owner -h <host> -U <user> -d <db> /tmp/latest_pg.gz
  4. 💾 Full MongoDB Restore (restore_mongodb_dump)
    - Uses mongorestore --drop for complete collection restore
    - Command: mongorestore --drop --archive=<file> --gzip --uri=<uri>
  5. ⚙️ Process MongoDB Data (process_mongodb_data)
    - Placeholder for your data transformation logic
    - Runs after both database restores complete

  Configuration Required:

  Set these Airflow Variables (use the template in /home/airflow/airflow/dags/config/airflow_variables.json):
  airflow variables set POSTGRES_HOST "your_postgres_host"
  airflow variables set POSTGRES_USER "your_postgres_user"
  airflow variables set POSTGRES_DB "your_postgres_database"
  airflow variables set MONGODB_HOST "your_mongodb_host"
  airflow variables set MONGODB_USER "your_mongodb_user"
  airflow variables set MONGODB_PASS "your_mongodb_password"


  Dependencies:

  Install required packages: pip install -r requirements_workflow.txt

  The workflow ensures complete database restoration with the --clean and --drop flags as requested, and handles multiple MongoDB databases automatically.