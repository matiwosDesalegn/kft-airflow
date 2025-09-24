# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Environment Setup

This is an **Apache Airflow 3.0.6** production environment with PostgreSQL backend. Activate the Python virtual environment before working:

```bash
source /home/airflow/venv/bin/activate
```

## Core Architecture

**Database Configuration:**
- **Backend**: PostgreSQL (AWS RDS: `de-ingester-instance-1.coo17ussvrhh.us-east-1.rds.amazonaws.com`)
- **Executor**: LocalExecutor (single-machine processing)
- **Authentication**: Simple auth manager with admin:admin credentials

**Key Directories:**
- `/home/airflow/airflow/dags/` - DAG definitions
- `/home/airflow/airflow/dags/config/` - Configuration templates
- `/home/airflow/airflow/plugins/` - Custom operators/hooks (ready for extension)
- `/home/airflow/airflow/logs/` - Execution logs

## DAG Patterns

**Simple Tutorial Pattern** (`hello_world.py`):
- Uses modern `schedule=timedelta(days=1)` syntax (not deprecated `schedule_interval`)
- Standard BashOperator usage with retry logic

**Production Workflow Pattern** (`full_database_restore_workflow.py`):
- Complex S3-based data pipeline for database restoration
- Multi-database support (PostgreSQL + MongoDB)
- XCom for inter-task communication
- Variable-based configuration management
- Manual trigger scheduling for destructive operations
- Parallel task execution with proper dependencies:
  ```python
  find_latest_postgres_dump_task >> restore_postgres_dump_task
  find_latest_mongodb_dump_task >> restore_mongodb_dump_task
  [restore_postgres_dump_task, restore_mongodb_dump_task] >> process_mongodb_data_task
  ```

## Configuration Management

**Airflow Variables Setup:**
Use the template in `/home/airflow/airflow/dags/config/airflow_variables.json` and set via CLI:
```bash
airflow variables set POSTGRES_HOST "your_postgres_host"
airflow variables set POSTGRES_USER "your_postgres_user"
airflow variables set POSTGRES_DB "your_postgres_database"
airflow variables set MONGODB_HOST "your_mongodb_host"
airflow variables set MONGODB_USER "your_mongodb_user"
airflow variables set MONGODB_PASS "your_mongodb_password"
```

**Dependencies:**
Install project-specific packages:
```bash
pip install -r requirements_workflow.txt
```
Core dependencies: `boto3>=1.26.0`, `pymongo>=4.0.0`, `psycopg2-binary>=2.9.0`

## Data Pipeline Architecture

**S3 Integration:**
- Staging bucket: `kft-lakehouse-staging`
- PostgreSQL backups: `digital_lending/backups/zemzem/postgres/`
- MongoDB backups: `digital_lending/backups/zemzem/mongodb/`
- Timestamp-based latest file selection using S3 LastModified

**Database Restore Commands:**
- PostgreSQL: `pg_restore --verbose --clean --no-acl --no-owner`
- MongoDB: `mongorestore --drop --archive --gzip`

## Development Conventions

**Error Handling:**
- Comprehensive try-catch blocks in Python callables
- Detailed logging with context information
- Fail-fast approach with meaningful error messages

**Security:**
- Database credentials externalized to Airflow Variables
- AWS credentials handled via IAM roles (not in code)
- Separate authentication file generation

**DAG Safety:**
- Use `manual` schedule for destructive operations
- Set `max_active_runs=1` for restore workflows
- Implement proper task dependencies to prevent race conditions

## System Requirements

- Python 3.12 runtime
- PostgreSQL client tools (`pg_restore`, `psql`)
- MongoDB client tools (`mongorestore`, `mongo`)
- AWS CLI access for S3 operations
- Virtual environment with Airflow 3.0.6 and providers