# ISMB — Índice de Sentimento do Mercado Brasileiro

Score diário de **0 a 100** que sintetiza o estado do mercado financeiro brasileiro. Agrega seis indicadores calculados a partir de séries históricas de preços, volume e NLP sobre notícias, combinados em um score final ponderado por tabela de banco de dados — sem alterar código.


---

## Sumário

- [Os seis indicadores](#os-seis-indicadores)
- [Arquitetura](#arquitetura)
- [Tecnologias](#tecnologias)
- [Estrutura do repositório](#estrutura-do-repositório)
- [Banco de dados](#banco-de-dados)
- [Factory — Pipeline de dados](#factory--pipeline-de-dados)
- [Backend — API REST](#backend--api-rest)
- [Frontend — Dashboard](#frontend--dashboard)
- [Setup local completo](#setup-local-completo)
- [Stack completa com Docker e Airflow](#stack-completa-com-docker-e-airflow)
- [Variáveis de ambiente](#variáveis-de-ambiente)
- [Testes e desenvolvimento](#testes-e-desenvolvimento)

---

## Os seis indicadores

Cada indicador é normalizado para a escala 0–100 antes de entrar no cálculo do índice final. Os pesos ficam na tabela `curated.dim_indicador`, coluna `peso_ismb` — para reponderar, basta atualizar a tabela e reprocessar a data, sem alterar código.

| Letra | Indicador | Fonte | Metodologia |
|:---:|---|---|---|
| A | Risco de Crédito | CDS Brasil 5Y | Volatilidade EWMA + rank de retorno |
| B | Retorno de Mercado | Ibovespa | Z-score do retorno diário (janela 21 dias) |
| C | Volatilidade de Mercado | IBOV + IVVB11 | EWMA 50% + ATR 30% + volatilidade IVVB 20% |
| D | Atividade de Mercado | Volume Ibovespa | Volume normalizado por média móvel de 30 dias |
| E | Confiança no Mercado Local | IFIX | Retorno ajustado do fundo de índice imobiliário |
| F | Sentimento de Notícias | InfoMoney, Valor Investe, Seu Dinheiro, Money Times | NLP sobre manchetes coletadas diariamente |

---

## Arquitetura

```
┌─────────────────────────────────────────────────────┐
│                    factory/                         │
│   Kedro 0.19 + Apache Airflow 3.0 (Docker)          │
│                                                     │
│   data_ingestion → data_validation → data_quality   │
│   → data_consolidation → data_processing            │
│   → data_score → data_analytics                     │
│                                                     │
│   Grava em PostgreSQL 13 (porta 5434)               │
└────────────────────────┬────────────────────────────┘
                         │ PostgreSQL
              ┌──────────┴──────────┐
              │                     │
┌─────────────▼──────┐   ┌──────────▼─────────┐
│   infra/backend/   │   │  infra/frontend/   │
│   FastAPI 0.116    │◄──│  Flask 2.2         │
│   porta 8000       │   │  porta 5000        │
│   /api/*           │   │  index.html        │
└────────────────────┘   └────────────────────┘
```

---

## Tecnologias

| Camada | Tecnologia | Versão |
|---|---|---|
| Pipeline de dados | Kedro | 0.19.14 |
| Orquestração | Apache Airflow | 3.0.1 |
| Banco de dados | PostgreSQL | 13 |
| Background API | FastAPI + Uvicorn | 0.116.0 / 0.35.0 |
| Frontend | Flask + Jinja2 | 2.2.5 / 3.1.6 |
| ORM / Driver | SQLAlchemy + psycopg2 | 1.4.54 / 2.9.10 |
| NLP | NLTK | 3.9.1 |
| Dados financeiros | yfinance | 0.2.65 |
| Scraping | BeautifulSoup4 + Requests | 4.13.4 / 2.32.4 |
| Containerização | Docker Compose | — |
| Runtime | Python | ≥ 3.9 (recomendado 3.12) |

---

## Estrutura do repositório

```
ismb-app/
├── factory/                         # Pipeline de dados (Kedro + Airflow)
│   ├── conf/
│   │   ├── base/                    # catalog.yml e parameters por pipeline
│   │   ├── airflow/                 # Overrides para execução no Airflow
│   │   │   ├── credentials.yml      # Conexão ismb-db:5432 (dentro do Docker)
│   │   │   └── dags/factory_dag.py  # DAG gerada pelo kedro-airflow
│   │   └── local/                   # Credenciais locais (gitignore)
│   ├── src/factory/
│   │   ├── pipelines/               # 7 pipelines Kedro
│   │   │   ├── data_ingestion/
│   │   │   ├── data_validation/
│   │   │   ├── data_quality/
│   │   │   ├── data_consolidation/
│   │   │   ├── data_processing/
│   │   │   ├── data_score/
│   │   │   └── data_analytics/
│   │   ├── datasets.py              # AppendSQLDataset (upsert/dedup)
│   │   ├── hooks.py                 # MonitoringHooks (governa execução)
│   │   ├── pipeline_registry.py     # Registro automático de pipelines
│   │   └── saiph/                   # SchemaRegistry e Monitor
│   ├── docker/ismb-db/init/         # SQL de inicialização (schemas, tabelas, dims)
│   ├── docker-compose.yaml          # Airflow + PostgreSQL ISMB + Kedro Viz
│   ├── Dockerfile                   # Imagem customizada com Kedro + dependências
│   └── requirements.txt
├── infra/
│   ├── backend/
│   │   ├── app/
│   │   │   ├── main.py              # FastAPI app factory + lifespan
│   │   │   ├── api/
│   │   │   │   ├── router.py        # Agrega todos os routers
│   │   │   │   └── endpoints/       # 9 módulos de endpoints
│   │   │   ├── core/
│   │   │   │   ├── config.py        # DatabaseConfig (env vars)
│   │   │   │   ├── cache.py         # Cache em memória
│   │   │   │   └── database.py      # Pool de conexões psycopg2
│   │   │   ├── models/              # Modelos Pydantic
│   │   │   └── services/
│   │   │       └── db_service.py    # Queries ao banco
│   │   └── clear_cache.py           # Script auxiliar para limpar cache
│   ├── frontend/
│   │   ├── run.py                   # Entrypoint Flask
│   │   └── app/
│   │       ├── routes.py            # Blueprint: / e /saibamais/<letra>
│   │       ├── templates/index.html
│   │       └── static/
│   └── requirements.txt
├── docs/indicators/                 # Documentação markdown de cada indicador
└── README.md
```

---

## Banco de dados

PostgreSQL 13 com seis schemas por responsabilidade. Inicializado automaticamente pelos scripts em `factory/docker/ismb-db/init/` na primeira subida do container.

| Schema | Responsabilidade |
|---|---|
| `stage` | Dados brutos ingeridos das fontes externas (landing zone) |
| `curated` | Data warehouse: dimensões (`dim_*`) e tabelas fato (`fato_*`) |
| `indicators` | Séries históricas calculadas dos seis indicadores |
| `analytics` | Visões pré-agregadas para o dashboard (`kpis`, `dashboard_diario`) |
| `governance` | Logs de execução dos pipelines, relatórios de qualidade e métricas |
| `sandbox` | Espelho de `curated` e `indicators` para dev e testes |

A série histórica começa em 2018 e é atualizada diariamente via Airflow.

> **Portas do banco:**  
> `5434` → instância de produção (`ismb` / banco `ismb`)  
> `5433` → instância de desenvolvimento (`ismb_dev`)

---

## Factory — Pipeline de dados

### Fluxo do pipeline

```
data_ingestion
     │  Coleta preços, volumes e notícias de APIs e scraping
     ▼
data_validation
     │  Valida regras de negócio antes do processamento
     ▼
data_quality
     │  Verifica gaps, nulos, outliers, consistência de schema
     ▼
data_consolidation
     │  Move stage → curated com normalização e tipagem
     ▼
data_processing
     │  Aplica as fórmulas dos 6 indicadores → indicators.*
     ▼
data_score
     │  Lê indicadores + pesos de dim_indicador → fato_indice_ismb
     ▼
data_analytics
     │  Agrega para as visões do dashboard → analytics.*
```

### AppendSQLDataset — idempotência por design

O dataset customizado em `src/factory/datasets.py` resolve o problema mais comum em pipelines financeiros: re-execução sem duplicação.

| Ambiente | Comportamento |
|---|---|
| `prd` | `INSERT ... ON CONFLICT DO UPDATE` em lotes de 1000 linhas |
| `sandbox`, `dev`, `test` | `TRUNCATE` + `INSERT` após deduplicação em memória pela PK natural |

Configuração no `catalog.yml`:

```yaml
stage.minha_tabela:
  type: factory.datasets.AppendSQLDataset
  table_name: "stage.minha_tabela"
  credentials: db_credentials
  pk_columns: [dat_ref, cod_indice]
  environment: ${environment}
  batch_size: 1000
```

### MonitoringHooks — governança de execução

O `MonitoringHooks` em `src/factory/hooks.py` instrumenta cada pipeline e cada node automaticamente. A cada execução registra em `governance.*`:

- Tempo de início e fim
- Status `SUCCESS` ou `FAILED`
- Parâmetros usados (`odate`, `environment`, `process_full_data`)
- Inputs e outputs do node

### Comandos Kedro

```bash
# Ativar ambiente e entrar na pasta
cd factory

# Rodar pipeline completo para uma data
kedro run --params="odate=2026-04-15"

# Rodar pipeline específico
kedro run --pipeline data_score --params="odate=2026-04-15"

# Reprocessar série histórica completa desde 2018
kedro run --params="process_full_data=true,odate=2026-04-15"

# Rodar em ambiente sandbox (sem afetar produção)
kedro run --env local --params="odate=2026-04-15,environment=sandbox"

# Visualizar grafo do pipeline
kedro viz run
```

### Parâmetros principais

| Parâmetro | Tipo | Padrão | Descrição |
|---|---|---|---|
| `odate` | `YYYY-MM-DD` | obrigatório | Data de referência do processamento |
| `environment` | string | `prd` | `prd`, `sandbox`, `dev`, `test` |
| `process_full_data` | bool | `false` | Reprocessa toda a série histórica desde 2018 |

---

## Backend — API REST

### Inicialização e cache

A aplicação FastAPI (`infra/backend/app/main.py`) usa um `lifespan` para:

1. Inicializar o pool de conexões PostgreSQL (mín. 2, máx. 10 por padrão)
2. Pré-carregar `read_indice` e `read_mercado` no cache em memória — garantindo primeira requisição instantânea

### Endpoints disponíveis

Todos os endpoints ficam sob o prefixo `/api`.

| Método | Rota | Descrição |
|---|---|---|
| `GET` | `/api/indice` | Série temporal do ISMB ou de um sub-score (A–F) |
| `GET` | `/api/resumo` | Últimas 2 observações para o card do dashboard |
| `GET` | `/api/serie_temporal` | Série do ISMB com indicadores técnicos |
| `GET` | `/api/correlacao` | Matriz de correlação entre entidades |
| `GET` | `/api/indicador_full/{nome}` | Série completa de um indicador específico |
| `GET` | `/api/mercado` | Dados diários de IBOV, IFIX, IVVB e CDS |
| `GET` | `/api/kpis` | KPIs anualizados (retorno, volatilidade, drawdown) |
| `GET` | `/api/dashboard_diario` | Métricas consolidadas do dashboard |
| `GET` | `/api/analytics` | Lista tabelas de analytics disponíveis |
| `GET` | `/api/analytics/{filename}` | Lê uma tabela de analytics |
| `GET` | `/api/quality` | Relatórios de qualidade de dados |
| `GET` | `/api/pipeline_logs` | Logs de execução dos nodes pelos pipelines |
| `POST` | `/api/cache/clear` | Limpa o cache em memória |
| `GET` | `/api/cache/status` | Status e TTL do cache |

**Nomes de indicadores** aceitos em `/api/indicador_full/{nome}`:
`risco_credito`, `retorno_mercado`, `volatilidade_mercado`, `atividade_mercado`, `confianca_mercado_local`, `sentimento_noticias`

### Documentação interativa

| UI | URL |
|---|---|
| Swagger UI | `http://localhost:8000/docs` |
| ReDoc | `http://localhost:8000/redoc` |

### Como rodar o backend

```bash
# A partir da raiz do repositório
cd infra/backend
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Ou com o módulo completo a partir da raiz:

```bash
uvicorn infra.backend.app.main:app --reload --host 0.0.0.0 --port 8000
```

### Limpar cache após pipeline

```bash
python infra/backend/clear_cache.py
```

---

## Frontend — Dashboard

Dashboard Flask que consome a API e renderiza o índice ISMB atual, a série histórica e os seis componentes.

| Rota | Descrição |
|---|---|
| `/` | Dashboard principal (index.html) |
| `/saibamais/<letra>` | Detalhe de cada indicador (A–F) renderizado do Markdown em `docs/indicators/` |

### Como rodar o frontend

```bash
python infra/frontend/run.py
```

Sobe em `http://localhost:5000` (Flask debug mode).

---

## Setup local completo

### Pré-requisitos

- Python ≥ 3.9 (recomendado: 3.12)
- Docker Desktop (com Docker Compose v2)

### 1. Banco de dados

Sobe apenas o PostgreSQL do ISMB (sem Airflow):

```bash
docker compose -f factory/docker-compose.yaml up -d ismb-db
```

Na primeira inicialização, os scripts em `factory/docker/ismb-db/init/` criam automaticamente todos os schemas, tabelas, índices e dados de dimensão (incluindo `dim_indicador` com os pesos padrão).

### 2. Ambiente Python

```bash
# Na raiz do repositório
python -m venv .venv

# Windows
.venv\Scripts\Activate.ps1

# Linux / macOS
source .venv/bin/activate

pip install -r factory/requirements.txt
pip install -r infra/requirements.txt
```

### 3. Credenciais do Kedro

Crie o arquivo `factory/conf/local/credentials.yml`:

```yaml
db_credentials:
  con: "postgresql+psycopg2://ismb:ismb@localhost:5434/ismb"
```

> A porta é **5434** (mapeada pelo docker-compose da instância de produção).

### 4. Rodar o pipeline

```bash
cd factory
kedro run --params="odate=2026-04-15"
```

Para carga histórica completa desde 2018 (demora vários minutos):

```bash
kedro run --params="process_full_data=true,odate=2026-04-15"
```

### 5. Rodar backend e frontend

```bash
# Terminal 1 — API
cd infra/backend
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

# Terminal 2 — Dashboard
python infra/frontend/run.py
```

| Serviço | URL |
|---|---|
| Dashboard | `http://localhost:5000` |
| API | `http://localhost:8000/api` |
| Swagger UI | `http://localhost:8000/docs` |

---

## Stack completa com Docker e Airflow

Para subir toda a infraestrutura incluindo Airflow para agendamento diário automático:

```bash
# Primeira vez: inicializar banco de metadados do Airflow
docker compose up airflow-init

# Subir stack completa
docker compose up
```

### Serviços provisionados

| Serviço | URL / Porta | Descrição |
|---|---|---|
| `ismb-db` | `localhost:5434` | PostgreSQL ISMB (produção) |
| `ismb-db-dev` | `localhost:5433` | PostgreSQL ISMB (desenvolvimento) |
| `airflow-apiserver` | `http://localhost:8080` | Airflow Web UI |
| `airflow-scheduler` | — | Scheduler (dispara DAGs) |
| `airflow-dag-processor` | — | Processador de DAGs |
| `kedro-viz` | `http://localhost:4141` | Visualizador do grafo do pipeline |
| `postgres` (airflow) | interno | Metastore do Airflow |

**Login Airflow padrão:** `airflow / airflow`

### DAG factory_ismb

A DAG em `factory/conf/airflow/dags/factory_dag.py` executa o pipeline Kedro completo diariamente. Cada node do Kedro vira um operador independente (`KedroOperator`), permitindo reprocessamento granular por node na UI do Airflow.

Parâmetros passáveis via **Trigger DAG w/ config** na UI:

```json
{
  "odate": "2026-04-15",
  "process_full_data": false,
  "environment": "prd"
}
```

Se `odate` não for informado, o Airflow usa a data de execução da DAG automaticamente (`{{ ds }}`).

### Parar a stack

```bash
docker compose -f factory/docker-compose.yaml down
```

---

## Variáveis de ambiente

| Variável | Componente | Padrão | Descrição |
|---|---|---|---|
| `DATABASE_URL` | Backend | `postgresql+psycopg2://ismb:ismb@localhost:5434/ismb` | String de conexão do backend |
| `DB_MIN_CONNECTIONS` | Backend | `2` | Conexões mínimas no pool |
| `DB_MAX_CONNECTIONS` | Backend | `10` | Conexões máximas no pool |
| `DB_QUERY_TIMEOUT` | Backend | `30` | Timeout de queries (segundos) |
| `ISMB_DB_CONN` | Factory (Docker) | — | Fallback para `localhost` dentro de container |
| `AIRFLOW_UID` | Airflow | `50000` | UID do usuário nos containers Airflow |

---

## Testes e desenvolvimento

```bash
cd factory

# Rodar todos os testes
pytest tests/ -v

# Com cobertura
pytest tests/ --cov=src/factory --cov-report=term-missing

# Lint
ruff check src/
ruff format src/
```

Os testes cobrem cada pipeline de forma isolada com fixtures locais e não dependem do banco de dados em execução.

Para inspecionar o grafo de pipelines localmente sem Docker:

```bash
cd factory
kedro viz run
# Abre em http://localhost:4141
```
