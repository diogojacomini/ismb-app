# Imagem base com Airflow
ARG BASE_IMAGE=apache/airflow:3.0.1
FROM ${BASE_IMAGE} AS base

# Instala ferramentas e dependências Python
USER root
RUN apt-get update && \
    apt-get install -y wget curl gnupg software-properties-common procps libkrb5-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Cria usuário não root seguro
ARG KEDRO_UID=999
ARG KEDRO_GID=1000
RUN groupadd -g ${KEDRO_GID} factory && \
    useradd -m -u ${KEDRO_UID} -g ${KEDRO_GID} factory

# Instala Python dependencies
USER airflow
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

WORKDIR /home/factory
COPY --chown=airflow:root . .

# Instala o pacote como usuário airflow
RUN pip install -e .

# Ajusta propriedade final
USER root
RUN mkdir -p /home/factory/data/sandbox && \
    chown -R ${KEDRO_UID}:${KEDRO_GID} /home/factory && \
    chmod -R 755 /home/factory

USER ${KEDRO_UID}:${KEDRO_GID}

EXPOSE 4141

CMD ["kedro", "viz", "--autoreload"]
