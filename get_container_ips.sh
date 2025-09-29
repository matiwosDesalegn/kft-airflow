#!/bin/bash

echo "=== Docker Container Network Information ==="
echo ""

# Check if containers are running
if ! docker ps | grep -q "airflow_postgres\|airflow_mongodb\|airflow_pgadmin"; then
    echo "⚠️  Containers not running. Start them with: docker-compose up -d"
    echo ""
fi

echo "📍 Container IP Addresses:"
echo "------------------------"

# PostgreSQL container
if docker ps | grep -q "airflow_postgres"; then
    PG_IP=$(docker inspect airflow_postgres | grep -E '"IPAddress".*[0-9]' | tail -1 | cut -d'"' -f4)
    echo "🐘 PostgreSQL Container: $PG_IP:5432"
    echo "   Database: airflow_restore"
    echo "   Username: postgres"
    echo "   Password: airflow123"
else
    echo "🐘 PostgreSQL Container: NOT RUNNING"
fi

echo ""

# MongoDB container
if docker ps | grep -q "airflow_mongodb"; then
    MONGO_IP=$(docker inspect airflow_mongodb | grep -E '"IPAddress".*[0-9]' | tail -1 | cut -d'"' -f4)
    echo "🍃 MongoDB Container: $MONGO_IP:27017"
    echo "   Username: mongouser"
    echo "   Password: mongo123"
    echo "   Auth Database: admin"
else
    echo "🍃 MongoDB Container: NOT RUNNING"
fi

echo ""

# pgAdmin container
if docker ps | grep -q "airflow_pgadmin"; then
    PGADMIN_IP=$(docker inspect airflow_pgadmin | grep -E '"IPAddress".*[0-9]' | tail -1 | cut -d'"' -f4)
    echo "🔧 pgAdmin Container: $PGADMIN_IP:80"
    echo "   Web Access: http://localhost:5050"
    echo "   Email: admin@admin.com"
    echo "   Password: root"
else
    echo "🔧 pgAdmin Container: NOT RUNNING"
fi

echo ""
echo "🌐 Host Machine Access (Recommended):"
echo "-------------------------------------"
echo "PostgreSQL: localhost:5432"
echo "MongoDB: localhost:27017"
echo "pgAdmin Web: http://localhost:5050"
echo ""

echo "🔗 Docker Network Information:"
echo "------------------------------"
docker network ls | grep -E "NETWORK|airflow"

echo ""
echo "💡 Connection Commands:"
echo "----------------------"
echo "# PostgreSQL via psql"
echo "PGPASSWORD=airflow123 psql -h localhost -p 5432 -U postgres -d airflow_restore"
echo ""
echo "# MongoDB via mongo shell"
echo "mongo mongodb://mongouser:mongo123@localhost:27017/admin"
echo ""
echo "# MongoDB Connection String for Compass"
echo "mongodb://mongouser:mongo123@localhost:27017/?authSource=admin"