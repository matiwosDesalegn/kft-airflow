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
- **Authentication**: Simple auth manager with admin:e6fwDvcdU6PBpG55 credentials

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

## ✅ Database Restoration Setup - COMPLETED

**Implementation Status: All phases successfully completed**

### ✅ Local Development Databases (Phase 1)
**Docker Services Configured** (`docker-compose.yaml`):
- PostgreSQL 15 service on port 5432 with airflow_restore database
- MongoDB 6.0 service on port 27017 with authentication
- pgAdmin service on port 5050 for database management
- Shared /tmp volume for restoration file access
- Persistent data volumes for both databases

**Airflow Variables Configured**:
- All 14 variables successfully imported via Airflow CLI
- Local database credentials: localhost:5432 (postgres/airflow123)
- MongoDB credentials: localhost:27017 (mongouser/mongo123)
- Production RDS credentials: de-ingester-instance-1.coo17ussvrhh.us-east-1.rds.amazonaws.com

### ✅ Database Restoration Workflow (Phase 2)
**Updated DAG** (`full_database_restore_workflow.py`):
- Fixed for Airflow 3.0.6 compatibility (schedule parameter, operator imports)
- Added POSTGRES_PASSWORD variable and PGPASSWORD authentication
- DAG successfully recognized by Airflow (no import errors)
- Dependencies installed: boto3>=1.26.0, pymongo>=4.0.0, psycopg2-binary>=2.9.0

**S3 Connectivity Verified**:
- AWS credentials working (access key ending Q7EU, region us-east-1)
- PostgreSQL backups accessible: s3://kft-lakehouse-staging/digital_lending/backups/zemzem/postgres/
- MongoDB backups accessible: s3://kft-lakehouse-staging/digital_lending/backups/zemzem/mongodb/
- Latest backup files confirmed (fineract_postgres_backup_* and customer-management_db_*)

### ✅ Production Integration (Phase 3)
**Production RDS Target Configured**:
- New task `load_to_production_rds` added to workflow
- Production database connection variables set
- Complete workflow: S3 Discovery → Local Restore → Data Processing → Production Load
- Task dependencies: [restore_postgres_dump_task, restore_mongodb_dump_task] >> process_mongodb_data_task >> load_to_production_task

**Security & Validation Complete**:
- AWS S3 bucket access confirmed with existing credentials
- Database connection variables externalized to Airflow Variables
- Password authentication implemented via PGPASSWORD environment variable

### 🚀 Ready to Use

## Step-by-Step Setup Process

1. **Fix Docker Permissions** (run as root or with sudo):
```bash
sudo usermod -aG docker airflow
sudo systemctl restart docker
# Then logout and login again, or run:
newgrp docker
```

2. **Start the Database Services**:
```bash
docker-compose up -d
```

3. **Get Network Information**:
```bash
./get_container_ips.sh
```

## Connection Configuration for External Tools

### pgAdmin Configuration:
1. Open `http://localhost:5050` in browser
2. Login with `admin@admin.com` / `root`
3. Add new server:
   - **Name**: Airflow PostgreSQL
   - **Host**: `localhost` (or container IP from script)
   - **Port**: `5432`
   - **Database**: `airflow_restore`
   - **Username**: `postgres`
   - **Password**: `airflow123`

### MongoDB Compass Configuration:
- **Connection String**: `mongodb://mongouser:mongo123@localhost:27017/?authSource=admin`
- Or manually:
  - **Host**: `localhost`
  - **Port**: `27017`
  - **Authentication**: Username/Password
  - **Username**: `mongouser`
  - **Password**: `mongo123`
  - **Authentication Database**: `admin`

### Database Access Details:
- **PostgreSQL**: `localhost:5432` (postgres/airflow123)
- **MongoDB**: `localhost:27017` (mongouser/mongo123)
- **pgAdmin Web**: `http://localhost:5050` (admin@admin.com/root)

The `get_container_ips.sh` script shows both container internal IPs and host machine access details once Docker is running.

**Run Database Restoration**:
```bash
# Activate environment and trigger workflow
source /home/airflow/venv/bin/activate
airflow dags trigger full_database_restore_workflow
```

**Workflow Stages**: S3 File Discovery → Local Database Restoration → Data Processing → Production RDS Loading

The implementation provides complete local testing capability with automatic production deployment to RDS instance.