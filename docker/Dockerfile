# Imagem base
FROM python:3.8-slim-buster

# Configurar variáveis de ambiente
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

ARG AIRFLOW_VERSION=2.6.1
ARG AIRFLOW_PYTHON_VERSION=3.8

ARG AIRFLOW_USER_HOME=/usr/local/airflow
ARG AIRFLOW_DAG_HOME=${AIRFLOW_USER_HOME}/dags
ARG AIRFLOW_PLUGGIN=${AIRFLOW_USER_HOME}/plugins
ARG AIRFLOW_DAG_PROJECT_HOME=${AIRFLOW_USER_HOME}

ENV AIRFLOW_DAG_PROJECT_HOME=${AIRFLOW_USER_HOME}
ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}
ENV TZ America/Sao_Paulo

ENV AIRFLOW_UID=50000
ENV AIRFLOW_GID=0

# Instalar dependências
RUN apt-get update && apt-get install -yqq --no-install-recommends \
    libpq-dev \
    freetds-bin \
    krb5-user \
    ldap-utils \
    build-essential \
    libssl-dev \
    libffi-dev \
    libffi6 \
    libsasl2-2 \
    libsasl2-modules \
    libssl1.1 \
    locales  \
    lsb-release \
    sasl2-bin \
    sqlite3 \
    unixodbc \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -ms /bin/bash -d ${AIRFLOW_USER_HOME} airflow 

# Instalação do Airflow
RUN pip install --upgrade pip
RUN pip install apache-airflow-providers-common-sql>=1.3.1
RUN pip install apache-airflow[sendgrid,postgres,google,ssh]==${AIRFLOW_VERSION} \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${AIRFLOW_PYTHON_VERSION}.txt"

# Copiar arquivos necessários
COPY docker/entrypoint.sh /entrypoint.sh
COPY docker/airflow.cfg /usr/local/airflow/airflow.cfg
COPY docker/requirements.txt /requirements.txt
COPY dags ${AIRFLOW_HOME}/dags
COPY docker/.env ${AIRFLOW_DAG_PROJECT_HOME}/.env
RUN chmod +x /entrypoint.sh

# Configurar o diretório de trabalho
RUN chown -R airflow: ${AIRFLOW_USER_HOME}
WORKDIR ${AIRFLOW_USER_HOME}

EXPOSE 80
EXPOSE 443
USER airflow

# Executar script de entrada
ENTRYPOINT ["sh", "/entrypoint.sh"]
