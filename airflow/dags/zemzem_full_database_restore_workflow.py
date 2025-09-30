from datetime import datetime, timedelta
import boto3
import os
import json
import logging
from typing import List, Tuple

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.models import Variable

# Data processing imports
import pandas as pd
from pymongo import MongoClient
from sqlalchemy import create_engine, text
import shutil

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'zemzem_full_database_restore_workflow',
    default_args=default_args,
    description='Zemzem full restore workflow for PostgreSQL and MongoDB from S3 dumps',
    schedule=None,  # Manual trigger
    catchup=False,
    max_active_runs=1,
    tags=['zemzem', 'restore', 'database', 's3', 'postgresql', 'mongodb']
)

# S3 Configuration
S3_BUCKET_STAGING = "kft-lakehouse-staging"
POSTGRES_S3_PREFIX = "digital_lending/backups/zemzem/postgres/"
POSTGRES_VALID_PREFIXES = ("fineract_postgres_backup_", "marketplace_postgres_backup_")
MONGODB_S3_PREFIX = "digital_lending/backups/zemzem/mongodb/"
MONGODB_DB_DUMP_PREFIXES = ["customer-management_db", "decision-db"]

def find_latest_s3_file(bucket_name: str, prefix: str, valid_prefixes: Tuple[str, ...] = None,
                       file_extension: str = '.tar') -> str:
    """
    Find the latest file in S3 bucket based on LastModified timestamp.

    Args:
        bucket_name: S3 bucket name
        prefix: S3 prefix to search in
        valid_prefixes: Tuple of valid prefixes for file names
        file_extension: File extension to filter by

    Returns:
        S3 key of the latest file
    """
    s3_client = boto3.client('s3')

    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

        if 'Contents' not in response:
            raise ValueError(f"No objects found in bucket {bucket_name} with prefix {prefix}")

        # Filter files based on valid prefixes and file extension
        valid_files = []
        for obj in response['Contents']:
            key = obj['Key']
            filename = key.split('/')[-1]  # Get filename from key

            if key.endswith(file_extension):
                if valid_prefixes:
                    if any(filename.startswith(vp) for vp in valid_prefixes):
                        valid_files.append(obj)
                else:
                    valid_files.append(obj)

        if not valid_files:
            raise ValueError(f"No valid files found with prefixes {valid_prefixes} and extension {file_extension}")

        # Sort by LastModified timestamp and get the latest
        latest_file = max(valid_files, key=lambda x: x['LastModified'])

        logging.info(f"Latest file found: {latest_file['Key']} (Modified: {latest_file['LastModified']})")
        return latest_file['Key']

    except Exception as e:
        logging.error(f"Error finding latest S3 file: {str(e)}")
        raise

def download_s3_file(bucket_name: str, s3_key: str, local_path: str) -> str:
    """
    Download file from S3 to local filesystem.

    Args:
        bucket_name: S3 bucket name
        s3_key: S3 key of the file to download
        local_path: Local path where file should be saved

    Returns:
        Path to downloaded file
    """
    s3_client = boto3.client('s3')

    try:
        # Ensure local directory exists
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        logging.info(f"Downloading {s3_key} from {bucket_name} to {local_path}")
        s3_client.download_file(bucket_name, s3_key, local_path)

        # Verify file was downloaded
        if not os.path.exists(local_path):
            raise ValueError(f"File was not downloaded successfully to {local_path}")

        file_size = os.path.getsize(local_path)
        logging.info(f"Successfully downloaded {local_path} (Size: {file_size} bytes)")

        return local_path

    except Exception as e:
        logging.error(f"Error downloading S3 file: {str(e)}")
        raise

def find_latest_postgres_dump(**context):
    """Find and download the latest PostgreSQL dump file from S3."""
    try:
        # Find latest PostgreSQL dump
        latest_s3_key = find_latest_s3_file(
            bucket_name=S3_BUCKET_STAGING,
            prefix=POSTGRES_S3_PREFIX,
            valid_prefixes=POSTGRES_VALID_PREFIXES,
            file_extension='.tar'
        )

        # Download the file
        local_path = '/tmp/latest_pg.tar'
        download_s3_file(S3_BUCKET_STAGING, latest_s3_key, local_path)

        # Store paths in XCom for next tasks (if running in Airflow context)
        if 'task_instance' in context:
            context['task_instance'].xcom_push(key='postgres_dump_path', value=local_path)
            context['task_instance'].xcom_push(key='postgres_s3_key', value=latest_s3_key)

        logging.info(f"PostgreSQL dump ready at: {local_path}")
        return local_path

    except Exception as e:
        logging.error(f"Failed to find/download PostgreSQL dump: {str(e)}")
        raise

def find_latest_mongodb_dump(**context):
    """Find and download the latest MongoDB dump files from S3."""
    try:
        downloaded_files = []

        for db_prefix in MONGODB_DB_DUMP_PREFIXES:
            # Find latest MongoDB dump for each database
            search_prefix = f"{MONGODB_S3_PREFIX}{db_prefix}"

            latest_s3_key = find_latest_s3_file(
                bucket_name=S3_BUCKET_STAGING,
                prefix=search_prefix,
                valid_prefixes=None,  # No specific prefix filtering for MongoDB
                file_extension='.tar'
            )

            # Download the file
            local_path = f'/tmp/{db_prefix}_latest.tar'
            download_s3_file(S3_BUCKET_STAGING, latest_s3_key, local_path)

            downloaded_files.append({
                'db_name': db_prefix,
                'local_path': local_path,
                's3_key': latest_s3_key
            })

        # Store information in XCom for next tasks (if running in Airflow context)
        if 'task_instance' in context:
            context['task_instance'].xcom_push(key='mongodb_dumps', value=downloaded_files)

        logging.info(f"MongoDB dumps ready: {[f['local_path'] for f in downloaded_files]}")
        return downloaded_files

    except Exception as e:
        logging.error(f"Failed to find/download MongoDB dumps: {str(e)}")
        raise

def process_mongodb_data(**context):
    """
    Process and transform MongoDB data after restore.
    Extracts data from restored MongoDB databases, transforms it, and loads into PostgreSQL.
    """
    try:
        logging.info("Starting MongoDB data processing and transformation...")

        # Get connection details from Airflow Variables
        mongo_host = Variable.get('MONGODB_HOST', 'localhost')
        mongo_port = int(Variable.get('MONGODB_PORT', '27017'))
        mongo_user = Variable.get('MONGODB_USER', 'mongouser')
        mongo_password = Variable.get('MONGODB_PASS', 'mongo123')

        # Production PostgreSQL connection details
        pg_host = Variable.get('PRODUCTION_POSTGRES_HOST')
        pg_port = int(Variable.get('PRODUCTION_POSTGRES_PORT', '5432'))
        pg_user = Variable.get('PRODUCTION_POSTGRES_USER', 'postgres')
        pg_password = Variable.get('PRODUCTION_POSTGRES_PASSWORD')
        pg_database = Variable.get('PRODUCTION_POSTGRES_DB', 'ansar')

        # MongoDB connection
        mongo_uri = f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}/?authSource=admin"
        client = MongoClient(mongo_uri)
        logging.info(f"Connected to MongoDB at {mongo_host}:{mongo_port}")

        # PostgreSQL connection
        from sqlalchemy import create_engine, text
        pg_engine = create_engine(
            f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_database}"
        )
        logging.info(f"Connected to PostgreSQL at {pg_host}:{pg_port}/{pg_database}")

        # Configuration
        db_names = ["customer-management_db", "decision-db"]
        output_dir = "/tmp/mongo_json_exports"
        os.makedirs(output_dir, exist_ok=True)

        # -----------------------------
        # Export MongoDB collections
        # -----------------------------
        logging.info("Starting MongoDB collection export...")
        for db_name in db_names:
            try:
                db = client[db_name]
                collections = db.list_collection_names()
                logging.info(f"Found collections in {db_name}: {collections}")

                for coll_name in collections:
                    logging.info(f"Exporting collection: {coll_name} from database: {db_name}")
                    data = list(db[coll_name].find())

                    # Convert ObjectId to string for JSON
                    for doc in data:
                        if "_id" in doc:
                            doc["_id"] = str(doc["_id"])

                    output_file = os.path.join(output_dir, f"{db_name}_{coll_name}.json")
                    with open(output_file, "w", encoding="utf-8") as f:
                        json.dump(data, f, indent=4)

                    logging.info(f"✅ Exported {len(data)} documents to {output_file}")
            except Exception as e:
                logging.error(f"Failed to export from {db_name}: {str(e)}")
                continue

        # -----------------------------
        # Process Customer Data
        # -----------------------------
        logging.info("Processing customer data...")
        try:
            customers_file = os.path.join(output_dir, "customer-management_db_customers.json")
            if os.path.exists(customers_file):
                with open(customers_file, "r", encoding="utf-8") as f:
                    data = json.load(f)

                # Process nested structures
                df_businesses = pd.json_normalize(data, record_path='businesses', meta=['_id'], record_prefix='business_', errors='ignore')
                if not df_businesses.empty and 'business_businessAddresses' in df_businesses.columns:
                    df_business_exploded = df_businesses.explode('business_businessAddresses')
                    df_business_addresses = pd.concat([
                        df_business_exploded.drop(columns=['business_businessAddresses']),
                        df_business_exploded['business_businessAddresses'].apply(pd.Series).add_prefix('business_address_')
                    ], axis=1)
                else:
                    df_business_addresses = df_businesses

                df_bank = pd.json_normalize(data, record_path='bankInformations', meta=['_id'], record_prefix='bank_', errors='ignore')
                df_personal_addresses = pd.json_normalize(data, record_path='personalAddresses', meta=['_id'], record_prefix='personal_address_', errors='ignore')
                df_educational_levels = pd.json_normalize(data, record_path='educationalLevels', meta=['_id'], record_prefix='educational_level_', errors='ignore')
                df_marital_statuses = pd.json_normalize(data, record_path='maritalStatuses', meta=['_id'], record_prefix='marital_status_', errors='ignore')

                # Main customer data
                df_main = pd.json_normalize(data)
                columns_to_drop = ['personalAddresses', 'bankInformations', 'educationalLevels', 'maritalStatuses', 'businesses']
                df_main_cleaned = df_main.drop(columns=[col for col in columns_to_drop if col in df_main.columns])

                # Merge all dataframes
                df_full = df_main_cleaned
                for df, suffix in [(df_personal_addresses, 'personal'), (df_bank, 'bank'),
                                  (df_educational_levels, 'educational'), (df_marital_statuses, 'marital'),
                                  (df_business_addresses, 'business')]:
                    if not df.empty:
                        df_full = df_full.merge(df, on="_id", how="left")

                # Clean column names
                column_renames = {
                    'educational_level_level': 'educational_level',
                    'marital_status_0': 'marital_status',
                    'business_businessName': 'business_Name',
                    'business_businessSector': 'business_Sector',
                    'business_businessRegisteredDate': 'business_RegisteredDate',
                    'business_businessRenewalDate': 'business_RenewalDate',
                    'business_businessLicenseNumber': 'business_LicenseNumber',
                    'userAccountInformation.currentStage': 'userAccountInformation_currentStage'
                }

                for old_col, new_col in column_renames.items():
                    if old_col in df_full.columns:
                        df_full.rename(columns={old_col: new_col}, inplace=True)

                logging.info(f"Customer data processed: {len(df_full)} rows, {len(df_full.columns)} columns")

                # Store into PostgreSQL
                with pg_engine.begin() as conn:
                    conn.execute(text("TRUNCATE TABLE IF EXISTS customers;"))
                    df_full.to_sql("customers", conn, if_exists="append", index=False)

                logging.info("✅ Customer data stored in PostgreSQL successfully")
            else:
                logging.warning(f"Customer data file not found: {customers_file}")
        except Exception as e:
            logging.error(f"Failed to process customer data: {str(e)}")

        # -----------------------------
        # Process Loan Processes Data
        # -----------------------------
        logging.info("Processing loan processes data...")
        try:
            loan_file = os.path.join(output_dir, "decision-db_loan-processes.json")
            if os.path.exists(loan_file):
                with open(loan_file, "r", encoding="utf-8") as f:
                    data_loan_processes = json.load(f)

                # Ensure it's a list
                if isinstance(data_loan_processes, dict):
                    data_loan_processes = [data_loan_processes]

                df_loan = pd.DataFrame(data_loan_processes)

                # Handle ordered_items if exists
                if "ordered_items" in df_loan.columns and not df_loan.empty:
                    df_exploded = df_loan.explode("ordered_items").reset_index(drop=True)
                    ordered_items_df = pd.json_normalize(df_exploded["ordered_items"])
                    df_exploded = df_exploded.drop(columns=["ordered_items"])
                    flattened_df = pd.concat([df_exploded.reset_index(drop=True), ordered_items_df], axis=1)
                else:
                    flattened_df = df_loan

                logging.info(f"Loan processes data processed: {len(flattened_df)} rows")

                # Store into PostgreSQL
                with pg_engine.begin() as conn:
                    conn.execute(text("TRUNCATE TABLE IF EXISTS loan_processes;"))
                    flattened_df.to_sql("loan_processes", conn, if_exists="append", index=False)

                logging.info("✅ Loan processes data stored in PostgreSQL successfully")
            else:
                logging.warning(f"Loan processes file not found: {loan_file}")
        except Exception as e:
            logging.error(f"Failed to process loan processes data: {str(e)}")

        # -----------------------------
        # Process Scoring History Data
        # -----------------------------
        logging.info("Processing scoring history data...")
        try:
            scoring_file = os.path.join(output_dir, "decision-db_scoring-history.json")
            if os.path.exists(scoring_file):
                with open(scoring_file, "r", encoding="utf-8") as f:
                    data_scoring = json.load(f)

                df_scoring = pd.json_normalize(data_scoring)

                # Handle ObjectId conversion
                if "_id.$oid" in df_scoring.columns:
                    df_scoring.rename(columns={"_id.$oid": "_id"}, inplace=True)
                elif "_id" in df_scoring.columns and not df_scoring.empty:
                    # Handle nested ObjectId structure
                    df_scoring["_id"] = df_scoring["_id"].apply(
                        lambda x: x.get("$oid") if isinstance(x, dict) and "$oid" in x else str(x)
                    )

                logging.info(f"Scoring history data processed: {len(df_scoring)} rows")

                # Store into PostgreSQL
                with pg_engine.begin() as conn:
                    conn.execute(text("TRUNCATE TABLE IF EXISTS scoring_history;"))
                    df_scoring.to_sql("scoring_history", conn, if_exists="append", index=False)

                logging.info("✅ Scoring history data stored in PostgreSQL successfully")
            else:
                logging.warning(f"Scoring history file not found: {scoring_file}")
        except Exception as e:
            logging.error(f"Failed to process scoring history data: {str(e)}")

        # Cleanup temporary files
        try:
            import shutil
            shutil.rmtree(output_dir)
            logging.info("Temporary files cleaned up")
        except Exception as e:
            logging.warning(f"Failed to cleanup temporary files: {str(e)}")

        # Close connections
        client.close()
        pg_engine.dispose()

        logging.info("MongoDB data processing completed successfully")
        return True

    except Exception as e:
        logging.error(f"Failed to process MongoDB data: {str(e)}")
        raise

# Task 1: Find Latest PostgreSQL Dump
find_latest_postgres_dump_task = PythonOperator(
    task_id='find_latest_postgres_dump',
    python_callable=find_latest_postgres_dump,
    dag=dag,
)

# Task 2: Find Latest MongoDB Dumps
find_latest_mongodb_dump_task = PythonOperator(
    task_id='find_latest_mongodb_dump',
    python_callable=find_latest_mongodb_dump,
    dag=dag,
)

# Task 3: Restore PostgreSQL Database (Full Restore with --clean)
restore_postgres_dump_task = BashOperator(
    task_id='restore_postgres_dump',
    bash_command="""
    # Get database connection details from Airflow Variables or connections
    PG_HOST="{{ var.value.get('POSTGRES_HOST', 'localhost') }}"
    PG_PORT="{{ var.value.get('POSTGRES_PORT', '5432') }}"
    PG_USER="{{ var.value.get('POSTGRES_USER', 'postgres') }}"
    PG_PASSWORD="{{ var.value.get('POSTGRES_PASSWORD', '') }}"
    PG_DB="{{ var.value.get('POSTGRES_DB', 'your_database') }}"

    echo "Starting full PostgreSQL restore with clean option..."
    echo "Target: $PG_HOST:$PG_PORT/$PG_DB as user $PG_USER"

    # Set PGPASSWORD environment variable for authentication
    export PGPASSWORD="$PG_PASSWORD"

    # Perform full restore with --clean flag to drop existing objects first
    pg_restore --verbose --clean --no-acl --no-owner \
        -h $PG_HOST \
        -p $PG_PORT \
        -U $PG_USER \
        -d $PG_DB \
        /tmp/latest_pg.tar

    if [ $? -eq 0 ]; then
        echo "PostgreSQL restore completed successfully"
    else
        echo "PostgreSQL restore failed"
        exit 1
    fi
    """,
    dag=dag,
)

# Task 4: Restore MongoDB Databases (Full Restore with --drop)
restore_mongodb_dump_task = BashOperator(
    task_id='restore_mongodb_dump',
    bash_command="""
    # Get MongoDB connection details from Airflow Variables
    MONGO_HOST="{{ var.value.get('MONGODB_HOST', 'localhost') }}"
    MONGO_PORT="{{ var.value.get('MONGODB_PORT', '27017') }}"
    MONGO_USER="{{ var.value.get('MONGODB_USER', '') }}"
    MONGO_PASS="{{ var.value.get('MONGODB_PASS', '') }}"

    echo "Starting full MongoDB restore with drop option..."

    # Restore each database dump
    for archive_file in /tmp/*_latest.tar; do
        if [ -f "$archive_file" ]; then
            db_name=$(basename "$archive_file" _latest.tar)
            echo "Restoring database: $db_name"
            echo "Archive file: $archive_file"

            # Build MongoDB URI
            if [ -n "$MONGO_USER" ] && [ -n "$MONGO_PASS" ]; then
                MONGO_URI="mongodb://$MONGO_USER:$MONGO_PASS@$MONGO_HOST:$MONGO_PORT/$db_name"
            else
                MONGO_URI="mongodb://$MONGO_HOST:$MONGO_PORT/$db_name"
            fi

            # Perform restore with --drop flag to remove existing collections
            mongorestore --drop \
                --archive="$archive_file" \
                --gzip \
                --uri="$MONGO_URI"

            if [ $? -eq 0 ]; then
                echo "MongoDB restore for $db_name completed successfully"
            else
                echo "MongoDB restore for $db_name failed"
                exit 1
            fi
        fi
    done

    echo "All MongoDB restores completed successfully"
    """,
    dag=dag,
)

# Task 5: Process and Transform Data
process_mongodb_data_task = PythonOperator(
    task_id='process_mongodb_data',
    python_callable=process_mongodb_data,
    dag=dag,
)

# Task 6: Load Processed Data to Production RDS
load_to_production_task = BashOperator(
    task_id='load_to_production_rds',
    bash_command="""
    # Get production database connection details from Airflow Variables
    PROD_PG_HOST="{{ var.value.get('PRODUCTION_POSTGRES_HOST', '') }}"
    PROD_PG_PORT="{{ var.value.get('PRODUCTION_POSTGRES_PORT', '5432') }}"
    PROD_PG_USER="{{ var.value.get('PRODUCTION_POSTGRES_USER', 'postgres') }}"
    PROD_PG_PASSWORD="{{ var.value.get('PRODUCTION_POSTGRES_PASSWORD', '') }}"
    PROD_PG_DB="{{ var.value.get('PRODUCTION_POSTGRES_DB', 'ansar') }}"

    echo "Loading processed data to production RDS..."
    echo "Target: $PROD_PG_HOST:$PROD_PG_PORT/$PROD_PG_DB as user $PROD_PG_USER"

    # Set PGPASSWORD environment variable for authentication
    export PGPASSWORD="$PROD_PG_PASSWORD"

    # Example: Connect to production database and run data processing queries
    # This is a placeholder - actual implementation would depend on your data processing needs
    psql -h $PROD_PG_HOST \
         -p $PROD_PG_PORT \
         -U $PROD_PG_USER \
         -d $PROD_PG_DB \
         -c "SELECT 'Production database connection successful' as status;"

    if [ $? -eq 0 ]; then
        echo "Production database connection verified successfully"
        echo "Ready for data processing and loading operations"
    else
        echo "Failed to connect to production database"
        exit 1
    fi
    """,
    dag=dag,
)

# Define task dependencies
find_latest_postgres_dump_task >> restore_postgres_dump_task
find_latest_mongodb_dump_task >> restore_mongodb_dump_task
[restore_postgres_dump_task, restore_mongodb_dump_task] >> process_mongodb_data_task >> load_to_production_task