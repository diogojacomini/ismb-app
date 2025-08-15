# ISMB - Índice de Sentimento do Mercado Brasileiro

O **ISMB (Índice de Sentimento do Mercado Brasileiro)** é uma análise de dados de mercado que gera indicadores e scores diários de sentimento financeiro. O projeto foi desenvolvido com **Kedro** para criação de pipelines modulares e reproduzíveis e é **orquestrado pelo Apache Airflow** para execução automatizada.  

Seguindo **boas práticas DevOps**, o ISMB garante:  
- **Reprodutibilidade:** pipelines que produzem resultados consistentes independentemente do ambiente.  
- **Versionamento:** controle de código, dados e configurações para rastreabilidade total.  
- **Infraestrutura como código (Terraform):** provisionamento de buckets, chaves e recursos de nuvem de forma automatizada e segura.  
- **Configuração por ambiente:** separação entre desenvolvimento, teste e produção.  
- **Execução em containers:** isolamento e portabilidade, garantindo consistência entre ambientes locais e de produção.

O ISMB permite:  
- Ingestão, processamento e pontuação de dados de mercado.  
- Monitoramento e visualização dos pipelines via **Kedro Viz**.  
- Orquestração robusta de workflows financeiros com **Airflow**.  


## Requisitos

- Linux ou WSL (Windows Subsystem for Linux)
- Python 3.12

## Como rodar localmente (sem Docker)

1) Clone o repositório

```bash
git clone https://github.com/diogojacomini/ismb-app.git
cd ismb-app
```

2) Crie e ative o ambiente virtual

Linux/WSL:
```bash
python -m venv .venv
source .venv/bin/activate
```

3) Instale as dependências

```bash
pip install -r factory/requirements.txt
```

Observação: para rodar local, ajuste os diretórios em `factory/conf/airflow/catalog.yml` (paths de dados conforme sua máquina).

4) Execute os pipelines Kedro (o único parâmetro obrigatório é `odate`; opcionais: `environment`, `process_full_data`)

```bash
kedro run --pipeline data_ingestion --params="odate=2025-08-14"
kedro run --pipeline data_processing --params="odate=2025-08-14"
kedro run --pipeline data_score --params="odate=2025-08-14"
```

Opcional (visualização):
```bash
kedro viz
```
Acesse: http://localhost:4141/

## Produção (Infraestrutura e credenciais)

1) Provisione recursos com Terraform (ex.: buckets e chaves de acesso)

2) Configure as credenciais no arquivo:

```
factory/conf/local/credentials.yml
```

## Orquestração com Apache Airflow (Docker Compose)

No diretório `factory/`:

```bash
docker-compose up airflow-init
docker-compose up
```

Serviços:
- Airflow Webserver: http://localhost:8080/
- Kedro Viz (quando iniciado localmente via `kedro viz`): http://localhost:4141/

Parâmetros da DAG: `odate` (YYYY-MM-DD) é obrigatório; `process_full_data` e outros são opcionais. Você pode acionar execuções e/ou passar `--conf` via Airflow UI ou CLI.

## Estrutura (visão geral)

- `factory/src/factory/` - código da aplicação e pipelines Kedro
- `factory/conf/` - configurações por ambiente (airflow/local), catálogo, parâmetros
- `factory/conf/airflow/dags/` - DAGs do Airflow
- `factory/docker-compose.yaml` e `factory/Dockerfile` - execução em containers
- `factory/src/factory/pipelines/` - todos os pipelines criados e disponiveis para executar
- `factory/data/` - dados locais (montados no container)

## Dicas e solução de problemas

- DateParseError com `{{ ds }}`: garanta que `odate` esteja definido como data real (ex.: `2025-08-15`) quando executar manualmente; nas DAGs, macros do Airflow são resolvidas automaticamente.
- Erros de caminho: ajuste `catalog.yml` para apontar para diretórios locais corretos.
- Dependências: use exatamente Python 3.12 e as versões de `factory/requirements.txt`.

## Sobre o Kedro

Kedro é um framework para criar pipelines de dados reprodutíveis e robustos. Ele organiza código, dados e configurações em uma estrutura padrão, facilita parametrização e integração com ferramentas como Airflow e Kedro Viz.

## Licença

Veja o arquivo `LICENSE` na raiz do repositório.
