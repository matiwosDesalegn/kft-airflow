from datetime import datetime, timedelta
import boto3
import os
import logging
from typing import List, Tuple

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.models import Variable

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
    'full_database_restore_workflow',
    default_args=default_args,
    description='Full restore workflow for PostgreSQL and MongoDB from S3 dumps',
    schedule=None,  # Manual trigger
    catchup=False,
    max_active_runs=1,
    tags=['restore', 'database', 's3', 'postgresql', 'mongodb']
)

# S3 Configuration
S3_BUCKET_STAGING = "kft-lakehouse-staging"
POSTGRES_S3_PREFIX = "digital_lending/backups/zemzem/postgres/"
POSTGRES_VALID_PREFIXES = ("fineract_postgres_backup_", "marketplace_postgres_backup_")
MONGODB_S3_PREFIX = "digital_lending/backups/zemzem/mongodb/"
MONGODB_DB_DUMP_PREFIXES = ["customer-management_db", "decision-db"]

def find_latest_s3_file(bucket_name: str, prefix: str, valid_prefixes: Tuple[str, ...] = None,
                       file_extension: str = '.gz') -> str:
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
            file_extension='.gz'
        )

        # Download the file
        local_path = '/tmp/latest_pg.gz'
        download_s3_file(S3_BUCKET_STAGING, latest_s3_key, local_path)

        # Store paths in XCom for next tasks
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
                file_extension='.archive'
            )

            # Download the file
            local_path = f'/tmp/{db_prefix}_latest.archive'
            download_s3_file(S3_BUCKET_STAGING, latest_s3_key, local_path)

            downloaded_files.append({
                'db_name': db_prefix,
                'local_path': local_path,
                's3_key': latest_s3_key
            })

        # Store information in XCom for next tasks
        context['task_instance'].xcom_push(key='mongodb_dumps', value=downloaded_files)

        logging.info(f"MongoDB dumps ready: {[f['local_path'] for f in downloaded_files]}")
        return downloaded_files

    except Exception as e:
        logging.error(f"Failed to find/download MongoDB dumps: {str(e)}")
        raise

def process_mongodb_data(**context):
    """
    Process and transform MongoDB data after restore.
    This is a placeholder - implement your specific data processing logic here.
    """
    try:
        logging.info("Starting MongoDB data processing and transformation...")

        # Get MongoDB dump information from previous task
        mongodb_dumps = context['task_instance'].xcom_pull(task_ids='find_latest_mongodb_dump', key='mongodb_dumps')

        # Example processing logic - replace with your actual implementation
        for dump_info in mongodb_dumps:
            db_name = dump_info['db_name']
            logging.info(f"Processing data from {db_name} database")

            # Your data transformation logic here
            # Example:
            # 1. Connect to MongoDB
            # 2. Read data from restored collections
            # 3. Apply transformations
            # 4. Load cleaned data into PostgreSQL tables

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
        /tmp/latest_pg.gz

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
    for archive_file in /tmp/*_latest.archive; do
        if [ -f "$archive_file" ]; then
            db_name=$(basename "$archive_file" _latest.archive)
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