-- ISMB Database - Schema: indicators
-- Indicadores calculados pelo pipeline data_processing. Cada tabela armazena a série histórica de um indicador
-- Ordem de execução: 04

SET search_path TO indicators, public;


-- -----------------------------------------------------------------------------
-- Tabela: indicators.indicador_risco_credito
-- Indicador de Risco de Crédito baseado no CDS Brasil 5Y
-- Código: RISCO_CREDITO

CREATE TABLE IF NOT EXISTS indicators.indicador_risco_credito (
    dat_ref              VARCHAR(10)   NOT NULL,
    cod_indicador        VARCHAR(32)   NOT NULL DEFAULT 'RISCO_CREDITO',
    retorno_diario       NUMERIC(12, 4),    -- retorno logarítmico diário do CDS
    vol_ewma             NUMERIC(12, 4),    -- volatilidade EWMA (lambda=0.94, janela=21)
    rank_vol             NUMERIC(8,  4),    -- rank percentil da volatilidade [0-1]
    rank_retorno         NUMERIC(8,  4),    -- rank percentil do retorno [0-1]
    risco_bruto          NUMERIC(8,  4),    -- composição ponderada rank_vol + rank_retorno
    score_risco_credito  NUMERIC(8,  2),    -- score final normalizado [0-100]
    CONSTRAINT pk_indicador_risco_credito PRIMARY KEY (dat_ref),
    CONSTRAINT chk_irc_score CHECK (score_risco_credito IS NULL OR score_risco_credito BETWEEN 0 AND 100)
);

COMMENT ON TABLE  indicators.indicador_risco_credito IS 'Indicador de Risco de Crédito (CDS Brasil 5Y).';
COMMENT ON COLUMN indicators.indicador_risco_credito.dat_ref              IS 'Data de referência do cálculo (formato YYYY-MM-DD)';
COMMENT ON COLUMN indicators.indicador_risco_credito.cod_indicador        IS 'Código do indicador (RISCO_CREDITO)';
COMMENT ON COLUMN indicators.indicador_risco_credito.retorno_diario       IS 'Retorno logarítmico diário do CDS Brasil 5Y';
COMMENT ON COLUMN indicators.indicador_risco_credito.vol_ewma             IS 'Volatilidade EWMA (lambda=0.94, janela=21 dias)';
COMMENT ON COLUMN indicators.indicador_risco_credito.rank_vol             IS 'Rank percentil da volatilidade [0-1]';
COMMENT ON COLUMN indicators.indicador_risco_credito.rank_retorno         IS 'Rank percentil do retorno [0-1]';
COMMENT ON COLUMN indicators.indicador_risco_credito.risco_bruto          IS 'Composição ponderada: rank_vol + rank_retorno';
COMMENT ON COLUMN indicators.indicador_risco_credito.score_risco_credito  IS 'Score final normalizado [0-100]: 0=baixo risco, 100=alto risco';

-- -----------------------------------------------------------------------------
-- Tabela: indicators.indicador_retorno_mercado
-- Indicador de Retorno do Mercado baseado no Ibovespa
-- Código: RETORNO_MERCADO

CREATE TABLE IF NOT EXISTS indicators.indicador_retorno_mercado (
    dat_ref               VARCHAR(10)   NOT NULL,
    cod_indicador         VARCHAR(32)   NOT NULL DEFAULT 'RETORNO_MERCADO',
    log_ret               NUMERIC(12, 4),    -- retorno logarítmico diário do IBOV
    media_ret             NUMERIC(12, 4),    -- média móvel do retorno (janela=21)
    desvio_ret            NUMERIC(12, 4),    -- desvio padrão móvel do retorno (janela=21)
    z_retorno             NUMERIC(10, 4),    -- z-score do retorno diário
    media_vol             NUMERIC(18, 4),    -- média móvel do volume (janela=30)
    score_retorno_mercado NUMERIC(8,  2),    -- score final normalizado [0-100]
    CONSTRAINT pk_indicador_retorno_mercado PRIMARY KEY (dat_ref),
    CONSTRAINT chk_irm_score CHECK (score_retorno_mercado IS NULL OR score_retorno_mercado BETWEEN 0 AND 100)
);

COMMENT ON TABLE  indicators.indicador_retorno_mercado IS 'Indicador de Retorno do Mercado (Ibovespa).';
COMMENT ON COLUMN indicators.indicador_retorno_mercado.dat_ref               IS 'Data de referência do cálculo (formato YYYY-MM-DD)';
COMMENT ON COLUMN indicators.indicador_retorno_mercado.cod_indicador         IS 'Código do indicador (RETORNO_MERCADO)';
COMMENT ON COLUMN indicators.indicador_retorno_mercado.log_ret               IS 'Retorno logarítmico diário do Ibovespa';
COMMENT ON COLUMN indicators.indicador_retorno_mercado.media_ret             IS 'Média móvel do retorno (janela=21 dias)';
COMMENT ON COLUMN indicators.indicador_retorno_mercado.desvio_ret            IS 'Desvio padrão móvel do retorno (janela=21 dias)';
COMMENT ON COLUMN indicators.indicador_retorno_mercado.z_retorno             IS 'Z-score do retorno diário em relação à janela de 21 dias';
COMMENT ON COLUMN indicators.indicador_retorno_mercado.media_vol             IS 'Média móvel do volume (janela=30 dias)';
COMMENT ON COLUMN indicators.indicador_retorno_mercado.score_retorno_mercado IS 'Score final normalizado [0-100]: 0=retorno negativo, 100=retorno positivo';

-- -----------------------------------------------------------------------------
-- Tabela: indicators.indicador_volatilidade_mercado
-- Indicador de Volatilidade do Mercado (EWMA + ATR + IVVB11)
-- Código: VOLATILIDADE

CREATE TABLE IF NOT EXISTS indicators.indicador_volatilidade_mercado (
    dat_ref                    VARCHAR(10)   NOT NULL,
    cod_indicador              VARCHAR(32)   NOT NULL DEFAULT 'VOLATILIDADE',
    log_ret                    NUMERIC(12, 4),    -- retorno logarítmico diário do IBOV
    ewma_var                   NUMERIC(14, 4),    -- variância EWMA (alpha=0.94)
    ewma_vol                   NUMERIC(10, 4),    -- volatilidade EWMA anualizada
    atr                        NUMERIC(14, 4),    -- Average True Range (janela=14)
    score_ewma                 NUMERIC(8,  4),    -- score componente EWMA [0-100]
    score_atr                  NUMERIC(8,  4),    -- score componente ATR [0-100]
    retorno_ivvb               NUMERIC(12, 4),    -- retorno logarítmico diário do IVVB11
    vol_ivvb                   NUMERIC(14, 4),    -- volatilidade EWMA do IVVB11
    score_ivvb                 NUMERIC(8,  2),    -- score componente IVVB [0-100]
    score_volatilidade_mercado NUMERIC(8,  2),    -- score final composto [0-100]
    CONSTRAINT pk_indicador_volatilidade_mercado PRIMARY KEY (dat_ref),
    CONSTRAINT chk_ivm_score CHECK (score_volatilidade_mercado IS NULL OR score_volatilidade_mercado BETWEEN 0 AND 100)
);

COMMENT ON TABLE  indicators.indicador_volatilidade_mercado IS 'Indicador de Volatilidade do Mercado (EWMA 50% + ATR 30% + IVVB 20%).';
COMMENT ON COLUMN indicators.indicador_volatilidade_mercado.dat_ref                    IS 'Data de referência do cálculo (formato YYYY-MM-DD)';
COMMENT ON COLUMN indicators.indicador_volatilidade_mercado.cod_indicador              IS 'Código do indicador (VOLATILIDADE)';
COMMENT ON COLUMN indicators.indicador_volatilidade_mercado.log_ret                    IS 'Retorno logarítmico diário do Ibovespa';
COMMENT ON COLUMN indicators.indicador_volatilidade_mercado.ewma_var                   IS 'Variância EWMA com alpha=0.94 (equivalente a janela ~17 dias)';
COMMENT ON COLUMN indicators.indicador_volatilidade_mercado.ewma_vol                   IS 'Volatilidade EWMA anualizada (sqrt(252) * sqrt(var))';
COMMENT ON COLUMN indicators.indicador_volatilidade_mercado.atr                        IS 'Average True Range (janela=14 dias)';
COMMENT ON COLUMN indicators.indicador_volatilidade_mercado.score_ewma                 IS 'Score componente EWMA [0-100]';
COMMENT ON COLUMN indicators.indicador_volatilidade_mercado.score_atr                  IS 'Score componente ATR [0-100]';
COMMENT ON COLUMN indicators.indicador_volatilidade_mercado.retorno_ivvb               IS 'Retorno logarítmico diário do IVVB11 (VIX Brasil)';
COMMENT ON COLUMN indicators.indicador_volatilidade_mercado.vol_ivvb                   IS 'Volatilidade EWMA do IVVB11';
COMMENT ON COLUMN indicators.indicador_volatilidade_mercado.score_ivvb                 IS 'Score componente IVVB [0-100]';
COMMENT ON COLUMN indicators.indicador_volatilidade_mercado.score_volatilidade_mercado IS 'Score final composto [0-100]: 0=baixa volatilidade, 100=alta volatilidade';

-- -----------------------------------------------------------------------------
-- Tabela: indicators.indicador_atividade_mercado
-- Indicador de Atividade do Mercado baseado em volume e desvio de retorno
-- Código: ATIVIDADE

CREATE TABLE IF NOT EXISTS indicators.indicador_atividade_mercado (
    dat_ref                 VARCHAR(10)   NOT NULL,
    cod_indicador           VARCHAR(32)   NOT NULL DEFAULT 'ATIVIDADE',
    retorno                 NUMERIC(12, 4),   -- retorno médio móvel (janela=3)
    desvio_relativo         NUMERIC(12, 4),   -- desvio do retorno em relação à média histórica
    score                   NUMERIC(8,  2),   -- score intermediário
    score_atividade_mercado NUMERIC(8,  2),   -- score final normalizado [0-100]
    CONSTRAINT pk_indicador_atividade_mercado PRIMARY KEY (dat_ref),
    CONSTRAINT chk_iam_score CHECK (score_atividade_mercado IS NULL OR score_atividade_mercado BETWEEN 0 AND 100)
);

COMMENT ON TABLE  indicators.indicador_atividade_mercado IS 'Indicador de Atividade do Mercado (volume + desvio de retorno).';
COMMENT ON COLUMN indicators.indicador_atividade_mercado.dat_ref                 IS 'Data de referência do cálculo (formato YYYY-MM-DD)';
COMMENT ON COLUMN indicators.indicador_atividade_mercado.cod_indicador           IS 'Código do indicador (ATIVIDADE)';
COMMENT ON COLUMN indicators.indicador_atividade_mercado.retorno                 IS 'Retorno percentual diário do Ibovespa';
COMMENT ON COLUMN indicators.indicador_atividade_mercado.desvio_relativo         IS 'Desvio do retorno em relação à média móvel (3 dias)';
COMMENT ON COLUMN indicators.indicador_atividade_mercado.score                   IS 'Score intermediário normalizado';
COMMENT ON COLUMN indicators.indicador_atividade_mercado.score_atividade_mercado IS 'Score final normalizado [0-100]: 0=baixa atividade, 100=alta atividade';

-- -----------------------------------------------------------------------------
-- Tabela: indicators.indicador_confianca_mercado_local
-- Indicador de Confiança do Mercado Local baseado no IFIX
-- Código: CONFIANCA_LOCAL

CREATE TABLE IF NOT EXISTS indicators.indicador_confianca_mercado_local (
    dat_ref                 VARCHAR(10)   NOT NULL,
    cod_indicador           VARCHAR(32)   NOT NULL DEFAULT 'CONFIANCA_LOCAL',
    retorno                 NUMERIC(12, 4),   -- retorno diário do IFIX
    retorno_mm              NUMERIC(12, 4),   -- média móvel do retorno (janela=3)
    desvio_relativo         NUMERIC(12, 4),   -- desvio relativo em relação à média histórica
    score_confianca_mercado NUMERIC(8,  2),   -- score final normalizado [0-100]
    CONSTRAINT pk_indicador_confianca_mercado_local PRIMARY KEY (dat_ref),
    CONSTRAINT chk_icml_score CHECK (score_confianca_mercado IS NULL OR score_confianca_mercado BETWEEN 0 AND 100)
);

COMMENT ON TABLE  indicators.indicador_confianca_mercado_local IS 'Indicador de Confiança do Mercado Local (IFIX).';
COMMENT ON COLUMN indicators.indicador_confianca_mercado_local.dat_ref                 IS 'Data de referência do cálculo (formato YYYY-MM-DD)';
COMMENT ON COLUMN indicators.indicador_confianca_mercado_local.cod_indicador           IS 'Código do indicador (CONFIANCA_LOCAL)';
COMMENT ON COLUMN indicators.indicador_confianca_mercado_local.retorno                 IS 'Retorno percentual diário do IFIX';
COMMENT ON COLUMN indicators.indicador_confianca_mercado_local.retorno_mm              IS 'Média móvel do retorno (janela=3 dias)';
COMMENT ON COLUMN indicators.indicador_confianca_mercado_local.desvio_relativo         IS 'Desvio relativo em relação à média histórica';
COMMENT ON COLUMN indicators.indicador_confianca_mercado_local.score_confianca_mercado IS 'Score final normalizado [0-100]: 0=baixa confiança, 100=alta confiança';

-- -----------------------------------------------------------------------------
-- Tabela: indicators.indicador_sentimento_noticias
-- Indicador de Sentimento de Mídia baseado em análise de notícias financeiras
-- Código: SENTIMENTO_NOTICIAS

CREATE TABLE IF NOT EXISTS indicators.indicador_sentimento_noticias (
    dat_ref         VARCHAR(10)   NOT NULL,
    cod_indicador   VARCHAR(32)   NOT NULL DEFAULT 'SENTIMENTO_NOTICIAS',
    score_noticias  NUMERIC(8, 2),     -- score final de sentimento [0-100]
    CONSTRAINT pk_indicador_sentimento_noticias PRIMARY KEY (dat_ref),
    CONSTRAINT chk_isn_score CHECK (score_noticias IS NULL OR score_noticias BETWEEN 0 AND 100)
);

COMMENT ON TABLE  indicators.indicador_sentimento_noticias IS 'Indicador de Sentimento de Notícias (NLP sobre portais financeiros).';
COMMENT ON COLUMN indicators.indicador_sentimento_noticias.dat_ref        IS 'Data de referência do cálculo (formato YYYY-MM-DD)';
COMMENT ON COLUMN indicators.indicador_sentimento_noticias.cod_indicador  IS 'Código do indicador (SENTIMENTO_NOTICIAS)';
COMMENT ON COLUMN indicators.indicador_sentimento_noticias.score_noticias IS 'Score final [0-100]: 0=sentimento muito negativo, 50=neutro, 100=muito positivo';


-- Índices - schema indicators

CREATE INDEX IF NOT EXISTS idx_brin_irc_dat  ON indicators.indicador_risco_credito           USING BRIN (dat_ref);
CREATE INDEX IF NOT EXISTS idx_brin_irm_dat  ON indicators.indicador_retorno_mercado         USING BRIN (dat_ref);
CREATE INDEX IF NOT EXISTS idx_brin_ivm_dat  ON indicators.indicador_volatilidade_mercado    USING BRIN (dat_ref);
CREATE INDEX IF NOT EXISTS idx_brin_iam_dat  ON indicators.indicador_atividade_mercado       USING BRIN (dat_ref);
CREATE INDEX IF NOT EXISTS idx_brin_icml_dat ON indicators.indicador_confianca_mercado_local USING BRIN (dat_ref);
CREATE INDEX IF NOT EXISTS idx_brin_isn_dat  ON indicators.indicador_sentimento_noticias     USING BRIN (dat_ref);
