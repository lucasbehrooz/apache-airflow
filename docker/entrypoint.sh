#!/usr/bin/env bash

export \
    AIRFLOW_HOME \
    AIRFLOW__CORE__EXECUTOR

# Install custom python package if requirements.txt is present
if [ -e "/requirements.txt" ]; then
    $(which pip) install --user -r /requirements.txt
fi

wait_for_port() {
    local name="$1" host="$2" port="$3"
    local j=0
    while ! nc -z "$host" "$port" >/dev/null 2>&1 </dev/null; do
        j=$((j + 1))
        if [ $j -ge $TRY_LOOP ]; then
            echo >&2 "$(date) - $host:$port still not reachable, giving up"
            exit 1
        fi
        echo "$(date) - waiting for $name... $j/$TRY_LOOP"
        sleep 5
    done
}
# Criando Usuario Admin Airflow (Usado Apenas a primeira vez quando não contem usuario no banco de dados)
airflow users create \
    --username airflow \
    --password airflow \
    --firstname Airflow \
    --lastname Admin \
    --role Admin \
    --email airflow@airflow.com.br

# Inicializa o scheduler do Airflow em segundo plano
airflow scheduler &

# Inicializa o webserver do Airflow em segundo plano
airflow webserver &

wait
