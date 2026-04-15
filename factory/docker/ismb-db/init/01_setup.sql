-- ISMB Database - Setup: Extensões e Schemas
-- Executado automaticamente na primeira inicialização do container ismb-db
-- Ordem de execução: 01


-- Extensões
CREATE EXTENSION IF NOT EXISTS pg_trgm;       -- busca textual por similaridade (noticias)
CREATE EXTENSION IF NOT EXISTS btree_gin;     -- índices GIN em colunas escalares
CREATE EXTENSION IF NOT EXISTS pgcrypto;      -- gen_random_uuid(), digest()

-- Schemas
CREATE SCHEMA IF NOT EXISTS governance;
CREATE SCHEMA IF NOT EXISTS stage;
CREATE SCHEMA IF NOT EXISTS curated;
CREATE SCHEMA IF NOT EXISTS indicators;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Permissões padrão
ALTER DEFAULT PRIVILEGES IN SCHEMA stage
    GRANT SELECT ON TABLES TO PUBLIC;

ALTER DEFAULT PRIVILEGES IN SCHEMA curated
    GRANT SELECT ON TABLES TO PUBLIC;

ALTER DEFAULT PRIVILEGES IN SCHEMA indicators
    GRANT SELECT ON TABLES TO PUBLIC;

ALTER DEFAULT PRIVILEGES IN SCHEMA analytics
    GRANT SELECT ON TABLES TO PUBLIC;

ALTER DEFAULT PRIVILEGES IN SCHEMA governance
    GRANT SELECT ON TABLES TO PUBLIC;
