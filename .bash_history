sudo apt update
sudo apt install python3-pip
sudo apt install sqlite3
sudo apt install python3.10-venv
sudo apt-get install libpq-dev
clear
sudo apt install python3.12-venv
python3 -m venv venv
source venv/bin/activate
pip install "apache-airflow[postgres]==2.5.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.5.0/constraints-3.12.txt"
clear
# Use Airflow 2.8.0 or later which supports Python 3.12
pip install "apache-airflow[postgres]==2.8.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.0/constraints-3.12.txt"
pip install "apache-airflow[postgres]==2.5.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.5.0/constraints-3.11.txt"
airflow db init
clear
pip install "apache-airflow[postgres]==2.5.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.5.0/constraints-3.7.txt"
clear
pip uninstall apache-airflow -y
pip install apache-airflow[postgres] --no-cache-dir
airflow db init
sudo airflow db init
airflow db migrate
airflow users create \
clear
airflow connections --help
airflow config list | grep -i user
airflow standalone
clear
nano ~/airflow/airflow.cfg
clear
nano ~/airflow/airflow.cfg
airflow db migrate
source venv/bin/activate
airflow db migrate
nano ~/airflow/airflow.cfg
airflow db migrate
clear
nano ~/airflow/airflow.cfg
clear
nano ~/airflow/airflow.cfg
airflow standalone --port 8081
clear
grep "web_server_port" ~/airflow/airflow.cfg
grep -n "web_server_port" ~/airflow/airflow.cfg
clear
# Look for web server related settings
grep -i "web" ~/airflow/airflow.cfg | head -10
grep -i "port" ~/airflow/airflow.cfg
airflow webserver --port 8081 &
airflow scheduler
clear
nano ~/airflow/airflow.cfg
airflow standalone
clear
# View the admin password
cat /home/airflow/airflow/simple_auth_manager_passwords.json.generated
nohup airflow standalone > airflow.log 2>&1 &
ps aux | grep airflow
ls
clear
exit
cd airflow
ls
clear
cd ~
mkdir -p .ssh
chmod 700 .ssh
sudo cp /home/ubuntu/.ssh/authorized_keys ~/.ssh/authorized_keys
sudo chown airflow:airflow ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
ls -la ~/.ssh/
cat ~/.ssh/authorized_keys
exit
tree -L 3
cd airflow
tree -L 3
cleare
clear
python ~/airflow/dags/hello_world.py
source ~/venv/bin/activate
airflow dags test hello_world 2024-01-01
clear
clear
ls
cd airflow/
ls
mkdir -p dags plugins include
tree -L 2
cat > ~/airflow/dags/hello_world.py << 'EOF'
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'hello_world',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

t2 = BashOperator(
    task_id='sleep',
    bash_command='sleep 5',
    retries=3,
    dag=dag,
)

t1 >> t2
EOF

mkdir -p ~/airflow/plugins/{hooks,operators,sensors,macros}
mkdir -p ~/airflow/include
clear
tree -L 3
