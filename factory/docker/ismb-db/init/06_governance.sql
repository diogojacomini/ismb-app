-- ISMB Database - Schema: governance
-- Tabelas de auditoria, controle de qualidade e validação de dados.
-- Ordem de execução: 06

SET search_path TO governance, public;

-- -----------------------------------------------------------------------------
-- Tabela: governance.pipeline_logs
-- Registro de cada execução de node/pipeline do Kedro

CREATE TABLE IF NOT EXISTS governance.pipeline_logs (
    run_id            VARCHAR(16)    NOT NULL,
    entity_name       VARCHAR(128)   NOT NULL,
    entity_type       VARCHAR(32)    NOT NULL,   -- 'node' | 'pipeline'
    status            VARCHAR(16)    NOT NULL,   -- 'SUCCESS' | 'FAILED' | 'SKIPPED'
    duration_seconds  NUMERIC(10, 3),
    start_time        TIMESTAMP WITHOUT TIME ZONE,
    end_time          TIMESTAMP WITHOUT TIME ZONE,
    metrics           JSONB          NOT NULL DEFAULT '{}',
    errors            JSONB          NOT NULL DEFAULT '[]',
    dat_ref           VARCHAR(10)    NOT NULL,
    environment       VARCHAR(32),
    process_full_data BOOLEAN,
    CONSTRAINT pk_pipeline_logs PRIMARY KEY (run_id, entity_name),
    CONSTRAINT chk_pipeline_logs_status
        CHECK (status IN ('SUCCESS', 'FAILED', 'SKIPPED', 'RUNNING'))
);

COMMENT ON TABLE  governance.pipeline_logs IS 'Log de execução de cada node/pipeline Kedro: status, duração, erros e métricas.';
COMMENT ON COLUMN governance.pipeline_logs.run_id            IS 'ID parcial do UUID de execução do pipeline (8 chars hexadecimais)';
COMMENT ON COLUMN governance.pipeline_logs.entity_name       IS 'Nome do node ou pipeline executado';
COMMENT ON COLUMN governance.pipeline_logs.entity_type       IS 'Tipo da entidade: node ou pipeline';
COMMENT ON COLUMN governance.pipeline_logs.status            IS 'Status da execução: SUCCESS | FAILED | SKIPPED | RUNNING';
COMMENT ON COLUMN governance.pipeline_logs.duration_seconds  IS 'Duração da execução em segundos';
COMMENT ON COLUMN governance.pipeline_logs.start_time        IS 'Timestamp de início da execução';
COMMENT ON COLUMN governance.pipeline_logs.end_time          IS 'Timestamp de término da execução';
COMMENT ON COLUMN governance.pipeline_logs.metrics           IS 'Métricas de execução em JSON (ex: número de registros processados)';
COMMENT ON COLUMN governance.pipeline_logs.errors            IS 'Lista de erros/warnings ocorridos durante a execução do node';
COMMENT ON COLUMN governance.pipeline_logs.dat_ref           IS 'Data de referência do processamento (formato YYYY-MM-DD)';
COMMENT ON COLUMN governance.pipeline_logs.environment       IS 'Ambiente de execução (prd, sandbox, dev, test, hk)';
COMMENT ON COLUMN governance.pipeline_logs.process_full_data IS 'Indica se processou dados completos (TRUE) ou apenas incremento (FALSE)';

-- -----------------------------------------------------------------------------
-- Tabela: governance.validation_data_mercado
-- Resultado das validações da camada curated para dados de mercado (IBOV, CDS…)

CREATE TABLE IF NOT EXISTS governance.validation_data_mercado (
    cod_indice              VARCHAR(16)    NOT NULL,
    dat_ref                 VARCHAR(10)    NOT NULL,
    n_rows                  INTEGER,
    ok                      BOOLEAN,
    date_min                TIMESTAMP WITHOUT TIME ZONE,
    date_max                TIMESTAMP WITHOUT TIME ZONE,
    unique_dates            INTEGER,
    null_count              INTEGER,
    null_pct                NUMERIC(6, 2),
    negative_volume_count   INTEGER,
    note                    TEXT,
    CONSTRAINT pk_validation_data_mercado PRIMARY KEY (cod_indice, dat_ref)
);

COMMENT ON TABLE  governance.validation_data_mercado IS 'Validação pós-processamento da camada curated para dados de mercado por índice e data.';
COMMENT ON COLUMN governance.validation_data_mercado.cod_indice            IS 'Código do índice validado (IBOV, CDS, IVVB, IFIX)';
COMMENT ON COLUMN governance.validation_data_mercado.dat_ref               IS 'Data de referência da validação (formato YYYY-MM-DD)';
COMMENT ON COLUMN governance.validation_data_mercado.n_rows                IS 'Número total de registros validados';
COMMENT ON COLUMN governance.validation_data_mercado.ok                    IS 'Indica se a validação passou sem erros críticos';
COMMENT ON COLUMN governance.validation_data_mercado.date_min              IS 'Data mínima encontrada no dataset';
COMMENT ON COLUMN governance.validation_data_mercado.date_max              IS 'Data máxima encontrada no dataset';
COMMENT ON COLUMN governance.validation_data_mercado.unique_dates          IS 'Número de datas únicas no dataset';
COMMENT ON COLUMN governance.validation_data_mercado.null_count            IS 'Quantidade total de valores nulos';
COMMENT ON COLUMN governance.validation_data_mercado.null_pct              IS 'Percentual de valores nulos';
COMMENT ON COLUMN governance.validation_data_mercado.negative_volume_count IS 'Quantidade de registros com volume negativo (erro de dados)';
COMMENT ON COLUMN governance.validation_data_mercado.note                  IS 'Observações adicionais ou mensagens de erro';

-- -----------------------------------------------------------------------------
-- Tabela: governance.validation_data_indicadores
-- Resultado das validações dos indicadores calculados pelo pipeline processing

CREATE TABLE IF NOT EXISTS governance.validation_data_indicadores (
    indicator     VARCHAR(64)    NOT NULL,
    score_col     VARCHAR(64)    NOT NULL,
    ok            BOOLEAN,
    dat_ref       VARCHAR(10)    NOT NULL,
    n_rows        INTEGER,
    unique_dates  INTEGER,
    date_min      TIMESTAMP WITHOUT TIME ZONE,
    date_max      TIMESTAMP WITHOUT TIME ZONE,
    null_count    INTEGER,
    null_pct      NUMERIC(6, 2),
    min           NUMERIC(14, 2),
    max           NUMERIC(14, 2),
    mean          NUMERIC(14, 2),
    std           NUMERIC(14, 2),
    CONSTRAINT pk_validation_data_indicadores PRIMARY KEY (indicator, score_col, dat_ref)
);

COMMENT ON TABLE  governance.validation_data_indicadores IS 'Validação estatística dos scores calculados para cada indicador do ISMB.';
COMMENT ON COLUMN governance.validation_data_indicadores.indicator     IS 'Nome do indicador validado';
COMMENT ON COLUMN governance.validation_data_indicadores.score_col     IS 'Nome da coluna de score validada';
COMMENT ON COLUMN governance.validation_data_indicadores.ok            IS 'Indica se a validação passou sem erros críticos';
COMMENT ON COLUMN governance.validation_data_indicadores.dat_ref       IS 'Data de referência da validação (formato YYYY-MM-DD)';
COMMENT ON COLUMN governance.validation_data_indicadores.n_rows        IS 'Número total de registros validados';
COMMENT ON COLUMN governance.validation_data_indicadores.unique_dates  IS 'Número de datas únicas no dataset';
COMMENT ON COLUMN governance.validation_data_indicadores.date_min      IS 'Data mínima encontrada no dataset';
COMMENT ON COLUMN governance.validation_data_indicadores.date_max      IS 'Data máxima encontrada no dataset';
COMMENT ON COLUMN governance.validation_data_indicadores.null_count    IS 'Quantidade de valores nulos no score';
COMMENT ON COLUMN governance.validation_data_indicadores.null_pct      IS 'Percentual de valores nulos';
COMMENT ON COLUMN governance.validation_data_indicadores.min           IS 'Valor mínimo do score';
COMMENT ON COLUMN governance.validation_data_indicadores.max           IS 'Valor máximo do score';
COMMENT ON COLUMN governance.validation_data_indicadores.mean          IS 'Média do score';
COMMENT ON COLUMN governance.validation_data_indicadores.std           IS 'Desvio padrão do score';

-- -----------------------------------------------------------------------------
-- Tabela: governance.validation_indice_isbm
-- Resultado da validação do índice ISMB composto

CREATE TABLE IF NOT EXISTS governance.validation_indice_isbm (
    indicator     VARCHAR(64)    NOT NULL,
    ok            BOOLEAN,
    dat_ref       VARCHAR(10)    NOT NULL,
    n_rows        INTEGER,
    unique_dates  INTEGER,
    date_min      TIMESTAMP WITHOUT TIME ZONE,
    date_max      TIMESTAMP WITHOUT TIME ZONE,
    null_count    INTEGER,
    out_of_range  INTEGER,
    min           NUMERIC(10, 2),
    max           NUMERIC(10, 2),
    mean          NUMERIC(10, 2),
    std           NUMERIC(10, 2),
    note          TEXT,
    CONSTRAINT pk_validation_indice_isbm PRIMARY KEY (indicator, dat_ref)
);

COMMENT ON TABLE  governance.validation_indice_isbm IS 'Validação do índice ISMB final: range, nulos, out-of-range e estatísticas.';
COMMENT ON COLUMN governance.validation_indice_isbm.indicator     IS 'Nome do indicador (sempre ISMB)';
COMMENT ON COLUMN governance.validation_indice_isbm.ok            IS 'Indica se a validação passou sem erros críticos';
COMMENT ON COLUMN governance.validation_indice_isbm.dat_ref       IS 'Data de referência da validação (formato YYYY-MM-DD)';
COMMENT ON COLUMN governance.validation_indice_isbm.n_rows        IS 'Número total de registros validados';
COMMENT ON COLUMN governance.validation_indice_isbm.unique_dates  IS 'Número de datas únicas no dataset';
COMMENT ON COLUMN governance.validation_indice_isbm.date_min      IS 'Data mínima encontrada no dataset';
COMMENT ON COLUMN governance.validation_indice_isbm.date_max      IS 'Data máxima encontrada no dataset';
COMMENT ON COLUMN governance.validation_indice_isbm.null_count    IS 'Quantidade de valores nulos no índice ISMB';
COMMENT ON COLUMN governance.validation_indice_isbm.out_of_range  IS 'Quantidade de valores fora do intervalo [0,100]';
COMMENT ON COLUMN governance.validation_indice_isbm.min           IS 'Valor mínimo do índice ISMB';
COMMENT ON COLUMN governance.validation_indice_isbm.max           IS 'Valor máximo do índice ISMB';
COMMENT ON COLUMN governance.validation_indice_isbm.mean          IS 'Média do índice ISMB no período';
COMMENT ON COLUMN governance.validation_indice_isbm.std           IS 'Desvio padrão do índice ISMB';
COMMENT ON COLUMN governance.validation_indice_isbm.note          IS 'Observações adicionais ou mensagens de erro';

-- -----------------------------------------------------------------------------
-- Tabela: governance.metrics_stage_cds

CREATE TABLE IF NOT EXISTS governance.metrics_stage_cds (
    metrics_id       VARCHAR(32)    NOT NULL,
    dataset_name     VARCHAR(64)    NOT NULL DEFAULT 'stage_cds',
    dat_ref          VARCHAR(10)    NOT NULL,
    check_timestamp  TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    total_records    INTEGER,
    null_count       INTEGER,
    duplicate_count  INTEGER,
    completeness_pct NUMERIC(6, 2),
    validity_pct     NUMERIC(6, 2),
    consistency_pct  NUMERIC(6, 2),
    outlier_count    INTEGER,
    quality_score    NUMERIC(6, 2),
    warnings         JSONB          NOT NULL DEFAULT '[]',
    errors           JSONB          NOT NULL DEFAULT '[]',
    status           VARCHAR(16)    CHECK (status IN ('OK', 'WARNING', 'ERROR')),
    CONSTRAINT pk_metrics_stage_cds PRIMARY KEY (metrics_id)
);

COMMENT ON TABLE  governance.metrics_stage_cds IS 'Métricas de qualidade de dados para stage_cds (CDS Brasil 5Y).';
COMMENT ON COLUMN governance.metrics_stage_cds.metrics_id       IS 'Identificador único da métrica';
COMMENT ON COLUMN governance.metrics_stage_cds.dataset_name     IS 'Nome do dataset (stage_cds)';
COMMENT ON COLUMN governance.metrics_stage_cds.dat_ref          IS 'Data de referência do processamento (formato YYYY-MM-DD)';
COMMENT ON COLUMN governance.metrics_stage_cds.check_timestamp  IS 'Timestamp da execução da checagem de qualidade';
COMMENT ON COLUMN governance.metrics_stage_cds.total_records    IS 'Número total de registros no dataset';
COMMENT ON COLUMN governance.metrics_stage_cds.null_count       IS 'Quantidade de valores nulos';
COMMENT ON COLUMN governance.metrics_stage_cds.duplicate_count  IS 'Quantidade de registros duplicados';
COMMENT ON COLUMN governance.metrics_stage_cds.completeness_pct IS 'Percentual de completude (campos preenchidos)';
COMMENT ON COLUMN governance.metrics_stage_cds.validity_pct     IS 'Percentual de validade (valores dentro do esperado)';
COMMENT ON COLUMN governance.metrics_stage_cds.consistency_pct  IS 'Percentual de consistência (relações entre campos)';
COMMENT ON COLUMN governance.metrics_stage_cds.outlier_count    IS 'Quantidade de outliers detectados';
COMMENT ON COLUMN governance.metrics_stage_cds.quality_score    IS 'Score global de qualidade [0-100]';
COMMENT ON COLUMN governance.metrics_stage_cds.warnings         IS 'Lista de advertências em formato JSON';
COMMENT ON COLUMN governance.metrics_stage_cds.errors           IS 'Lista de erros críticos em formato JSON';
COMMENT ON COLUMN governance.metrics_stage_cds.status           IS 'Status da checagem: OK | WARNING | ERROR';

-- -----------------------------------------------------------------------------
-- Tabela: governance.metrics_stage_ibov

CREATE TABLE IF NOT EXISTS governance.metrics_stage_ibov (
    metrics_id       VARCHAR(32)    NOT NULL,
    dataset_name     VARCHAR(64)    NOT NULL DEFAULT 'stage_ibov',
    dat_ref          VARCHAR(10)    NOT NULL,
    check_timestamp  TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    total_records    INTEGER,
    null_count       INTEGER,
    duplicate_count  INTEGER,
    completeness_pct NUMERIC(6, 2),
    validity_pct     NUMERIC(6, 2),
    consistency_pct  NUMERIC(6, 2),
    outlier_count    INTEGER,
    quality_score    NUMERIC(6, 2),
    warnings         JSONB          NOT NULL DEFAULT '[]',
    errors           JSONB          NOT NULL DEFAULT '[]',
    status           VARCHAR(16)    CHECK (status IN ('OK', 'WARNING', 'ERROR')),
    CONSTRAINT pk_metrics_stage_ibov PRIMARY KEY (metrics_id)
);

COMMENT ON TABLE  governance.metrics_stage_ibov IS 'Métricas de qualidade de dados para stage_ibov (Ibovespa).';
COMMENT ON COLUMN governance.metrics_stage_ibov.metrics_id       IS 'Identificador único da métrica';
COMMENT ON COLUMN governance.metrics_stage_ibov.dataset_name     IS 'Nome do dataset (stage_ibov)';
COMMENT ON COLUMN governance.metrics_stage_ibov.dat_ref          IS 'Data de referência do processamento (formato YYYY-MM-DD)';
COMMENT ON COLUMN governance.metrics_stage_ibov.check_timestamp  IS 'Timestamp da execução da checagem de qualidade';
COMMENT ON COLUMN governance.metrics_stage_ibov.total_records    IS 'Número total de registros no dataset';
COMMENT ON COLUMN governance.metrics_stage_ibov.null_count       IS 'Quantidade de valores nulos';
COMMENT ON COLUMN governance.metrics_stage_ibov.duplicate_count  IS 'Quantidade de registros duplicados';
COMMENT ON COLUMN governance.metrics_stage_ibov.completeness_pct IS 'Percentual de completude (campos preenchidos)';
COMMENT ON COLUMN governance.metrics_stage_ibov.validity_pct     IS 'Percentual de validade (valores dentro do esperado)';
COMMENT ON COLUMN governance.metrics_stage_ibov.consistency_pct  IS 'Percentual de consistência (relações entre campos)';
COMMENT ON COLUMN governance.metrics_stage_ibov.outlier_count    IS 'Quantidade de outliers detectados';
COMMENT ON COLUMN governance.metrics_stage_ibov.quality_score    IS 'Score global de qualidade [0-100]';
COMMENT ON COLUMN governance.metrics_stage_ibov.warnings         IS 'Lista de advertências em formato JSON';
COMMENT ON COLUMN governance.metrics_stage_ibov.errors           IS 'Lista de erros críticos em formato JSON';
COMMENT ON COLUMN governance.metrics_stage_ibov.status           IS 'Status da checagem: OK | WARNING | ERROR';

-- -----------------------------------------------------------------------------
-- Tabela: governance.metrics_stage_ivvb

CREATE TABLE IF NOT EXISTS governance.metrics_stage_ivvb (
    metrics_id       VARCHAR(32)    NOT NULL,
    dataset_name     VARCHAR(64)    NOT NULL DEFAULT 'stage_ivvb',
    dat_ref          VARCHAR(10)    NOT NULL,
    check_timestamp  TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    total_records    INTEGER,
    null_count       INTEGER,
    duplicate_count  INTEGER,
    completeness_pct NUMERIC(6, 2),
    validity_pct     NUMERIC(6, 2),
    consistency_pct  NUMERIC(6, 2),
    outlier_count    INTEGER,
    quality_score    NUMERIC(6, 2),
    warnings         JSONB          NOT NULL DEFAULT '[]',
    errors           JSONB          NOT NULL DEFAULT '[]',
    status           VARCHAR(16)    CHECK (status IN ('OK', 'WARNING', 'ERROR')),
    CONSTRAINT pk_metrics_stage_ivvb PRIMARY KEY (metrics_id)
);

COMMENT ON TABLE  governance.metrics_stage_ivvb IS 'Métricas de qualidade de dados para stage_ivvb (IVVB11 - VIX Brasil).';
COMMENT ON COLUMN governance.metrics_stage_ivvb.metrics_id       IS 'Identificador único da métrica';
COMMENT ON COLUMN governance.metrics_stage_ivvb.dataset_name     IS 'Nome do dataset (stage_ivvb)';
COMMENT ON COLUMN governance.metrics_stage_ivvb.dat_ref          IS 'Data de referência do processamento (formato YYYY-MM-DD)';
COMMENT ON COLUMN governance.metrics_stage_ivvb.check_timestamp  IS 'Timestamp da execução da checagem de qualidade';
COMMENT ON COLUMN governance.metrics_stage_ivvb.total_records    IS 'Número total de registros no dataset';
COMMENT ON COLUMN governance.metrics_stage_ivvb.null_count       IS 'Quantidade de valores nulos';
COMMENT ON COLUMN governance.metrics_stage_ivvb.duplicate_count  IS 'Quantidade de registros duplicados';
COMMENT ON COLUMN governance.metrics_stage_ivvb.completeness_pct IS 'Percentual de completude (campos preenchidos)';
COMMENT ON COLUMN governance.metrics_stage_ivvb.validity_pct     IS 'Percentual de validade (valores dentro do esperado)';
COMMENT ON COLUMN governance.metrics_stage_ivvb.consistency_pct  IS 'Percentual de consistência (relações entre campos)';
COMMENT ON COLUMN governance.metrics_stage_ivvb.outlier_count    IS 'Quantidade de outliers detectados';
COMMENT ON COLUMN governance.metrics_stage_ivvb.quality_score    IS 'Score global de qualidade [0-100]';
COMMENT ON COLUMN governance.metrics_stage_ivvb.warnings         IS 'Lista de advertências em formato JSON';
COMMENT ON COLUMN governance.metrics_stage_ivvb.errors           IS 'Lista de erros críticos em formato JSON';
COMMENT ON COLUMN governance.metrics_stage_ivvb.status           IS 'Status da checagem: OK | WARNING | ERROR';

-- -----------------------------------------------------------------------------
-- Tabela: governance.metrics_stage_ifix

CREATE TABLE IF NOT EXISTS governance.metrics_stage_ifix (
    metrics_id       VARCHAR(32)    NOT NULL,
    dataset_name     VARCHAR(64)    NOT NULL DEFAULT 'stage_ifix',
    dat_ref          VARCHAR(10)    NOT NULL,
    check_timestamp  TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    total_records    INTEGER,
    null_count       INTEGER,
    duplicate_count  INTEGER,
    completeness_pct NUMERIC(6, 2),
    validity_pct     NUMERIC(6, 2),
    consistency_pct  NUMERIC(6, 2),
    outlier_count    INTEGER,
    quality_score    NUMERIC(6, 2),
    warnings         JSONB          NOT NULL DEFAULT '[]',
    errors           JSONB          NOT NULL DEFAULT '[]',
    status           VARCHAR(16)    CHECK (status IN ('OK', 'WARNING', 'ERROR')),
    CONSTRAINT pk_metrics_stage_ifix PRIMARY KEY (metrics_id)
);

COMMENT ON TABLE  governance.metrics_stage_ifix IS 'Métricas de qualidade de dados para stage_ifix (IFIX - Fundos Imobiliários).';
COMMENT ON COLUMN governance.metrics_stage_ifix.metrics_id       IS 'Identificador único da métrica';
COMMENT ON COLUMN governance.metrics_stage_ifix.dataset_name     IS 'Nome do dataset (stage_ifix)';
COMMENT ON COLUMN governance.metrics_stage_ifix.dat_ref          IS 'Data de referência do processamento (formato YYYY-MM-DD)';
COMMENT ON COLUMN governance.metrics_stage_ifix.check_timestamp  IS 'Timestamp da execução da checagem de qualidade';
COMMENT ON COLUMN governance.metrics_stage_ifix.total_records    IS 'Número total de registros no dataset';
COMMENT ON COLUMN governance.metrics_stage_ifix.null_count       IS 'Quantidade de valores nulos';
COMMENT ON COLUMN governance.metrics_stage_ifix.duplicate_count  IS 'Quantidade de registros duplicados';
COMMENT ON COLUMN governance.metrics_stage_ifix.completeness_pct IS 'Percentual de completude (campos preenchidos)';
COMMENT ON COLUMN governance.metrics_stage_ifix.validity_pct     IS 'Percentual de validade (valores dentro do esperado)';
COMMENT ON COLUMN governance.metrics_stage_ifix.consistency_pct  IS 'Percentual de consistência (relações entre campos)';
COMMENT ON COLUMN governance.metrics_stage_ifix.outlier_count    IS 'Quantidade de outliers detectados';
COMMENT ON COLUMN governance.metrics_stage_ifix.quality_score    IS 'Score global de qualidade [0-100]';
COMMENT ON COLUMN governance.metrics_stage_ifix.warnings         IS 'Lista de advertências em formato JSON';
COMMENT ON COLUMN governance.metrics_stage_ifix.errors           IS 'Lista de erros críticos em formato JSON';
COMMENT ON COLUMN governance.metrics_stage_ifix.status           IS 'Status da checagem: OK | WARNING | ERROR';

-- -----------------------------------------------------------------------------
-- Tabela: governance.metrics_stage_infomoney

CREATE TABLE IF NOT EXISTS governance.metrics_stage_infomoney (
    metrics_id       VARCHAR(32)    NOT NULL,
    dataset_name     VARCHAR(64)    NOT NULL DEFAULT 'stage_infomoney',
    dat_ref          VARCHAR(10)    NOT NULL,
    check_timestamp  TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    total_records    INTEGER,
    null_count       INTEGER,
    duplicate_count  INTEGER,
    completeness_pct NUMERIC(6, 2),
    validity_pct     NUMERIC(6, 2),
    consistency_pct  NUMERIC(6, 2),
    outlier_count    INTEGER,
    quality_score    NUMERIC(6, 2),
    warnings         JSONB          NOT NULL DEFAULT '[]',
    errors           JSONB          NOT NULL DEFAULT '[]',
    status           VARCHAR(16)    CHECK (status IN ('OK', 'WARNING', 'ERROR')),
    CONSTRAINT pk_metrics_stage_infomoney PRIMARY KEY (metrics_id)
);

COMMENT ON TABLE  governance.metrics_stage_infomoney IS 'Métricas de qualidade de dados para stage_infomoney (Notícias InfoMoney).';
COMMENT ON COLUMN governance.metrics_stage_infomoney.metrics_id       IS 'Identificador único da métrica';
COMMENT ON COLUMN governance.metrics_stage_infomoney.dataset_name     IS 'Nome do dataset (stage_infomoney)';
COMMENT ON COLUMN governance.metrics_stage_infomoney.dat_ref          IS 'Data de referência do processamento (formato YYYY-MM-DD)';
COMMENT ON COLUMN governance.metrics_stage_infomoney.check_timestamp  IS 'Timestamp da execução da checagem de qualidade';
COMMENT ON COLUMN governance.metrics_stage_infomoney.total_records    IS 'Número total de registros no dataset';
COMMENT ON COLUMN governance.metrics_stage_infomoney.null_count       IS 'Quantidade de valores nulos';
COMMENT ON COLUMN governance.metrics_stage_infomoney.duplicate_count  IS 'Quantidade de registros duplicados';
COMMENT ON COLUMN governance.metrics_stage_infomoney.completeness_pct IS 'Percentual de completude (campos preenchidos)';
COMMENT ON COLUMN governance.metrics_stage_infomoney.validity_pct     IS 'Percentual de validade (valores dentro do esperado)';
COMMENT ON COLUMN governance.metrics_stage_infomoney.consistency_pct  IS 'Percentual de consistência (relações entre campos)';
COMMENT ON COLUMN governance.metrics_stage_infomoney.outlier_count    IS 'Quantidade de outliers detectados';
COMMENT ON COLUMN governance.metrics_stage_infomoney.quality_score    IS 'Score global de qualidade [0-100]';
COMMENT ON COLUMN governance.metrics_stage_infomoney.warnings         IS 'Lista de advertências em formato JSON';
COMMENT ON COLUMN governance.metrics_stage_infomoney.errors           IS 'Lista de erros críticos em formato JSON';
COMMENT ON COLUMN governance.metrics_stage_infomoney.status           IS 'Status da checagem: OK | WARNING | ERROR';

-- -----------------------------------------------------------------------------
-- Tabela: governance.metrics_stage_valorinveste

CREATE TABLE IF NOT EXISTS governance.metrics_stage_valorinveste (
    metrics_id       VARCHAR(32)    NOT NULL,
    dataset_name     VARCHAR(64)    NOT NULL DEFAULT 'stage_valorinveste',
    dat_ref          VARCHAR(10)    NOT NULL,
    check_timestamp  TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    total_records    INTEGER,
    null_count       INTEGER,
    duplicate_count  INTEGER,
    completeness_pct NUMERIC(6, 2),
    validity_pct     NUMERIC(6, 2),
    consistency_pct  NUMERIC(6, 2),
    outlier_count    INTEGER,
    quality_score    NUMERIC(6, 2),
    warnings         JSONB          NOT NULL DEFAULT '[]',
    errors           JSONB          NOT NULL DEFAULT '[]',
    status           VARCHAR(16)    CHECK (status IN ('OK', 'WARNING', 'ERROR')),
    CONSTRAINT pk_metrics_stage_valorinveste PRIMARY KEY (metrics_id)
);

COMMENT ON TABLE  governance.metrics_stage_valorinveste IS 'Métricas de qualidade de dados para stage_valorinveste (Notícias Valor Investe).';
COMMENT ON COLUMN governance.metrics_stage_valorinveste.metrics_id       IS 'Identificador único da métrica';
COMMENT ON COLUMN governance.metrics_stage_valorinveste.dataset_name     IS 'Nome do dataset (stage_valorinveste)';
COMMENT ON COLUMN governance.metrics_stage_valorinveste.dat_ref          IS 'Data de referência do processamento (formato YYYY-MM-DD)';
COMMENT ON COLUMN governance.metrics_stage_valorinveste.check_timestamp  IS 'Timestamp da execução da checagem de qualidade';
COMMENT ON COLUMN governance.metrics_stage_valorinveste.total_records    IS 'Número total de registros no dataset';
COMMENT ON COLUMN governance.metrics_stage_valorinveste.null_count       IS 'Quantidade de valores nulos';
COMMENT ON COLUMN governance.metrics_stage_valorinveste.duplicate_count  IS 'Quantidade de registros duplicados';
COMMENT ON COLUMN governance.metrics_stage_valorinveste.completeness_pct IS 'Percentual de completude (campos preenchidos)';
COMMENT ON COLUMN governance.metrics_stage_valorinveste.validity_pct     IS 'Percentual de validade (valores dentro do esperado)';
COMMENT ON COLUMN governance.metrics_stage_valorinveste.consistency_pct  IS 'Percentual de consistência (relações entre campos)';
COMMENT ON COLUMN governance.metrics_stage_valorinveste.outlier_count    IS 'Quantidade de outliers detectados';
COMMENT ON COLUMN governance.metrics_stage_valorinveste.quality_score    IS 'Score global de qualidade [0-100]';
COMMENT ON COLUMN governance.metrics_stage_valorinveste.warnings         IS 'Lista de advertências em formato JSON';
COMMENT ON COLUMN governance.metrics_stage_valorinveste.errors           IS 'Lista de erros críticos em formato JSON';
COMMENT ON COLUMN governance.metrics_stage_valorinveste.status           IS 'Status da checagem: OK | WARNING | ERROR';

-- -----------------------------------------------------------------------------
-- Tabela: governance.metrics_stage_seudinheiro

CREATE TABLE IF NOT EXISTS governance.metrics_stage_seudinheiro (
    metrics_id       VARCHAR(32)    NOT NULL,
    dataset_name     VARCHAR(64)    NOT NULL DEFAULT 'stage_seudinheiro',
    dat_ref          VARCHAR(10)    NOT NULL,
    check_timestamp  TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    total_records    INTEGER,
    null_count       INTEGER,
    duplicate_count  INTEGER,
    completeness_pct NUMERIC(6, 2),
    validity_pct     NUMERIC(6, 2),
    consistency_pct  NUMERIC(6, 2),
    outlier_count    INTEGER,
    quality_score    NUMERIC(6, 2),
    warnings         JSONB          NOT NULL DEFAULT '[]',
    errors           JSONB          NOT NULL DEFAULT '[]',
    status           VARCHAR(16)    CHECK (status IN ('OK', 'WARNING', 'ERROR')),
    CONSTRAINT pk_metrics_stage_seudinheiro PRIMARY KEY (metrics_id)
);

COMMENT ON TABLE  governance.metrics_stage_seudinheiro IS 'Métricas de qualidade de dados para stage_seudinheiro (Notícias Seu Dinheiro).';
COMMENT ON COLUMN governance.metrics_stage_seudinheiro.metrics_id       IS 'Identificador único da métrica';
COMMENT ON COLUMN governance.metrics_stage_seudinheiro.dataset_name     IS 'Nome do dataset (stage_seudinheiro)';
COMMENT ON COLUMN governance.metrics_stage_seudinheiro.dat_ref          IS 'Data de referência do processamento (formato YYYY-MM-DD)';
COMMENT ON COLUMN governance.metrics_stage_seudinheiro.check_timestamp  IS 'Timestamp da execução da checagem de qualidade';
COMMENT ON COLUMN governance.metrics_stage_seudinheiro.total_records    IS 'Número total de registros no dataset';
COMMENT ON COLUMN governance.metrics_stage_seudinheiro.null_count       IS 'Quantidade de valores nulos';
COMMENT ON COLUMN governance.metrics_stage_seudinheiro.duplicate_count  IS 'Quantidade de registros duplicados';
COMMENT ON COLUMN governance.metrics_stage_seudinheiro.completeness_pct IS 'Percentual de completude (campos preenchidos)';
COMMENT ON COLUMN governance.metrics_stage_seudinheiro.validity_pct     IS 'Percentual de validade (valores dentro do esperado)';
COMMENT ON COLUMN governance.metrics_stage_seudinheiro.consistency_pct  IS 'Percentual de consistência (relações entre campos)';
COMMENT ON COLUMN governance.metrics_stage_seudinheiro.outlier_count    IS 'Quantidade de outliers detectados';
COMMENT ON COLUMN governance.metrics_stage_seudinheiro.quality_score    IS 'Score global de qualidade [0-100]';
COMMENT ON COLUMN governance.metrics_stage_seudinheiro.warnings         IS 'Lista de advertências em formato JSON';
COMMENT ON COLUMN governance.metrics_stage_seudinheiro.errors           IS 'Lista de erros críticos em formato JSON';
COMMENT ON COLUMN governance.metrics_stage_seudinheiro.status           IS 'Status da checagem: OK | WARNING | ERROR';

-- -----------------------------------------------------------------------------
-- Tabela: governance.metrics_stage_moneytimes

CREATE TABLE IF NOT EXISTS governance.metrics_stage_moneytimes (
    metrics_id       VARCHAR(32)    NOT NULL,
    dataset_name     VARCHAR(64)    NOT NULL DEFAULT 'stage_moneytimes',
    dat_ref          VARCHAR(10)    NOT NULL,
    check_timestamp  TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    total_records    INTEGER,
    null_count       INTEGER,
    duplicate_count  INTEGER,
    completeness_pct NUMERIC(6, 2),
    validity_pct     NUMERIC(6, 2),
    consistency_pct  NUMERIC(6, 2),
    outlier_count    INTEGER,
    quality_score    NUMERIC(6, 2),
    warnings         JSONB          NOT NULL DEFAULT '[]',
    errors           JSONB          NOT NULL DEFAULT '[]',
    status           VARCHAR(16)    CHECK (status IN ('OK', 'WARNING', 'ERROR')),
    CONSTRAINT pk_metrics_stage_moneytimes PRIMARY KEY (metrics_id)
);

COMMENT ON TABLE  governance.metrics_stage_moneytimes IS 'Métricas de qualidade de dados para stage_moneytimes (Notícias Money Times).';
COMMENT ON COLUMN governance.metrics_stage_moneytimes.metrics_id       IS 'Identificador único da métrica';
COMMENT ON COLUMN governance.metrics_stage_moneytimes.dataset_name     IS 'Nome do dataset (stage_moneytimes)';
COMMENT ON COLUMN governance.metrics_stage_moneytimes.dat_ref          IS 'Data de referência do processamento (formato YYYY-MM-DD)';
COMMENT ON COLUMN governance.metrics_stage_moneytimes.check_timestamp  IS 'Timestamp da execução da checagem de qualidade';
COMMENT ON COLUMN governance.metrics_stage_moneytimes.total_records    IS 'Número total de registros no dataset';
COMMENT ON COLUMN governance.metrics_stage_moneytimes.null_count       IS 'Quantidade de valores nulos';
COMMENT ON COLUMN governance.metrics_stage_moneytimes.duplicate_count  IS 'Quantidade de registros duplicados';
COMMENT ON COLUMN governance.metrics_stage_moneytimes.completeness_pct IS 'Percentual de completude (campos preenchidos)';
COMMENT ON COLUMN governance.metrics_stage_moneytimes.validity_pct     IS 'Percentual de validade (valores dentro do esperado)';
COMMENT ON COLUMN governance.metrics_stage_moneytimes.consistency_pct  IS 'Percentual de consistência (relações entre campos)';
COMMENT ON COLUMN governance.metrics_stage_moneytimes.outlier_count    IS 'Quantidade de outliers detectados';
COMMENT ON COLUMN governance.metrics_stage_moneytimes.quality_score    IS 'Score global de qualidade [0-100]';
COMMENT ON COLUMN governance.metrics_stage_moneytimes.warnings         IS 'Lista de advertências em formato JSON';
COMMENT ON COLUMN governance.metrics_stage_moneytimes.errors           IS 'Lista de erros críticos em formato JSON';
COMMENT ON COLUMN governance.metrics_stage_moneytimes.status           IS 'Status da checagem: OK | WARNING | ERROR';

-- -----------------------------------------------------------------------------
-- Tabela: governance.data_quality_report
-- Relatório consolidado de qualidade com contadores de warnings/errors.

CREATE TABLE IF NOT EXISTS governance.data_quality_report (
    metrics_id        VARCHAR(32)    NOT NULL,
    dataset_name      VARCHAR(64)    NOT NULL,
    dat_ref           VARCHAR(10)    NOT NULL,
    check_timestamp   TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    total_records     INTEGER,
    null_count        INTEGER,
    duplicate_count   INTEGER,
    completeness_pct  NUMERIC(6, 2),
    validity_pct      NUMERIC(6, 2),
    consistency_pct   NUMERIC(6, 2),
    outlier_count     INTEGER,
    quality_score     NUMERIC(6, 2),
    warnings          JSONB          NOT NULL DEFAULT '[]',
    errors            JSONB          NOT NULL DEFAULT '[]',
    status            VARCHAR(16)    CHECK (status IN ('OK', 'WARNING', 'ERROR')),
    warning_count     INTEGER        NOT NULL DEFAULT 0,
    error_count       INTEGER        NOT NULL DEFAULT 0,
    execution_date    TIMESTAMP WITHOUT TIME ZONE,
    CONSTRAINT pk_data_quality_report PRIMARY KEY (metrics_id)
);

COMMENT ON TABLE  governance.data_quality_report IS 'Relatório consolidado de qualidade de dados para todos os datasets do pipeline.';
COMMENT ON COLUMN governance.data_quality_report.metrics_id        IS 'Identificador único da métrica';
COMMENT ON COLUMN governance.data_quality_report.dataset_name      IS 'Nome do dataset validado';
COMMENT ON COLUMN governance.data_quality_report.dat_ref           IS 'Data de referência do processamento (formato YYYY-MM-DD)';
COMMENT ON COLUMN governance.data_quality_report.check_timestamp   IS 'Timestamp da execução da checagem de qualidade';
COMMENT ON COLUMN governance.data_quality_report.total_records     IS 'Número total de registros no dataset';
COMMENT ON COLUMN governance.data_quality_report.null_count        IS 'Quantidade de valores nulos';
COMMENT ON COLUMN governance.data_quality_report.duplicate_count   IS 'Quantidade de registros duplicados';
COMMENT ON COLUMN governance.data_quality_report.completeness_pct  IS 'Percentual de completude (campos preenchidos)';
COMMENT ON COLUMN governance.data_quality_report.validity_pct      IS 'Percentual de validade (valores dentro do esperado)';
COMMENT ON COLUMN governance.data_quality_report.consistency_pct   IS 'Percentual de consistência (relações entre campos)';
COMMENT ON COLUMN governance.data_quality_report.outlier_count     IS 'Quantidade de outliers detectados';
COMMENT ON COLUMN governance.data_quality_report.quality_score     IS 'Score global de qualidade do dataset [0-100]';
COMMENT ON COLUMN governance.data_quality_report.warnings          IS 'Lista de advertências em formato JSON';
COMMENT ON COLUMN governance.data_quality_report.errors            IS 'Lista de erros críticos em formato JSON';
COMMENT ON COLUMN governance.data_quality_report.status            IS 'Status da checagem: OK | WARNING | ERROR';
COMMENT ON COLUMN governance.data_quality_report.warning_count     IS 'Número de advertências geradas durante a checagem de qualidade';
COMMENT ON COLUMN governance.data_quality_report.error_count       IS 'Número de erros críticos encontrados';
COMMENT ON COLUMN governance.data_quality_report.execution_date    IS 'Timestamp de execução do relatório de qualidade';


-- Índices - schema governance

-- pipeline_logs
CREATE INDEX IF NOT EXISTS idx_pl_dat          ON governance.pipeline_logs (dat_ref DESC);
CREATE INDEX IF NOT EXISTS idx_pl_status_fail  ON governance.pipeline_logs (dat_ref DESC)
    WHERE status = 'FAILURE';
CREATE INDEX IF NOT EXISTS idx_pl_entity       ON governance.pipeline_logs (entity_name, dat_ref DESC);

-- validations
CREATE INDEX IF NOT EXISTS idx_vdm_dat         ON governance.validation_data_mercado     (dat_ref DESC);
CREATE INDEX IF NOT EXISTS idx_vdi_dat         ON governance.validation_data_indicadores (dat_ref DESC);
CREATE INDEX IF NOT EXISTS idx_vii_dat         ON governance.validation_indice_isbm      (dat_ref DESC);

-- metrics_stage_* - índice em dat_ref para relatórios por período
CREATE INDEX IF NOT EXISTS idx_ms_cds_dat      ON governance.metrics_stage_cds          (dat_ref DESC);
CREATE INDEX IF NOT EXISTS idx_ms_ibov_dat     ON governance.metrics_stage_ibov         (dat_ref DESC);
CREATE INDEX IF NOT EXISTS idx_ms_ivvb_dat     ON governance.metrics_stage_ivvb         (dat_ref DESC);
CREATE INDEX IF NOT EXISTS idx_ms_ifix_dat     ON governance.metrics_stage_ifix         (dat_ref DESC);
CREATE INDEX IF NOT EXISTS idx_ms_info_dat     ON governance.metrics_stage_infomoney    (dat_ref DESC);
CREATE INDEX IF NOT EXISTS idx_ms_valor_dat    ON governance.metrics_stage_valorinveste (dat_ref DESC);
CREATE INDEX IF NOT EXISTS idx_ms_seud_dat     ON governance.metrics_stage_seudinheiro  (dat_ref DESC);
CREATE INDEX IF NOT EXISTS idx_ms_money_dat    ON governance.metrics_stage_moneytimes   (dat_ref DESC);

-- data_quality_report
CREATE INDEX IF NOT EXISTS idx_dqr_dataset_dat ON governance.data_quality_report  (dataset_name, dat_ref DESC);
CREATE INDEX IF NOT EXISTS idx_dqr_warn_error  ON governance.data_quality_report  (dat_ref DESC)
    WHERE status IN ('WARNING', 'ERROR');
