-- ISMB Database - Schema: analytics
-- Visões analíticas e consolidados gerados pelo pipeline data_analytics.
-- Ordem de execução: 05

SET search_path TO analytics, public;

-- -----------------------------------------------------------------------------
-- Tabela: analytics.analytics_dashboard_diario
-- Visão diária combinada: preço do índice + valor do ISMB + metadados da dimensão

CREATE TABLE IF NOT EXISTS analytics.analytics_dashboard_diario (
    cod_indice      VARCHAR(16)   NOT NULL,
    dat_ref         VARCHAR(10)   NOT NULL,
    close           NUMERIC(14, 2),           -- preço de fechamento do índice
    prev_close      NUMERIC(14, 2),           -- fechamento do dia anterior
    abs_change      NUMERIC(14, 2),           -- variação absoluta (close - prev_close)
    pct_change      NUMERIC(10, 2),           -- variação percentual
    volume          NUMERIC(20, 2),           -- volume negociado (NULL para índices sem volume)
    ismb_value      NUMERIC(8,  2),           -- valor do índice ISMB no dia
    ismb_abs_change NUMERIC(10, 2),           -- variação absoluta do ISMB
    ismb_pct_change NUMERIC(10, 2),           -- variação percentual do ISMB
    ind_sk_indice   SMALLINT,                 -- surrogate key do índice (desnorm.)
    ind_nome_indice VARCHAR(128),             -- nome do índice (desnorm.)
    ind_categoria   VARCHAR(64),              -- categoria (desnorm.)
    ind_descricao   TEXT,                     -- descrição (desnorm.)
    CONSTRAINT pk_analytics_dashboard_diario PRIMARY KEY (cod_indice, dat_ref)
);

COMMENT ON TABLE  analytics.analytics_dashboard_diario IS 'Tabela desnormalizada para dashboard diário: preço dos índices + valor ISMB por dia.';
COMMENT ON COLUMN analytics.analytics_dashboard_diario.cod_indice      IS 'Código do índice (IBOV, CDS, IVVB, IFIX)';
COMMENT ON COLUMN analytics.analytics_dashboard_diario.dat_ref         IS 'Data de referência (formato YYYY-MM-DD)';
COMMENT ON COLUMN analytics.analytics_dashboard_diario.close           IS 'Preço de fechamento do índice no dia';
COMMENT ON COLUMN analytics.analytics_dashboard_diario.prev_close      IS 'Preço de fechamento do dia anterior';
COMMENT ON COLUMN analytics.analytics_dashboard_diario.abs_change      IS 'Variação absoluta do preço de fechamento em relação ao dia anterior';
COMMENT ON COLUMN analytics.analytics_dashboard_diario.pct_change      IS 'Variação percentual do preço de fechamento em relação ao dia anterior';
COMMENT ON COLUMN analytics.analytics_dashboard_diario.volume          IS 'Volume negociado (NULL para índices sem volume como CDS e IFIX)';
COMMENT ON COLUMN analytics.analytics_dashboard_diario.ismb_value      IS 'Valor do índice ISMB no dia (escala 0-100)';
COMMENT ON COLUMN analytics.analytics_dashboard_diario.ismb_abs_change IS 'Variação absoluta do ISMB em relação ao dia anterior';
COMMENT ON COLUMN analytics.analytics_dashboard_diario.ismb_pct_change IS 'Variação percentual do ISMB em relação ao dia anterior';
COMMENT ON COLUMN analytics.analytics_dashboard_diario.ind_sk_indice   IS 'Surrogate key do índice (desnormalizado de dim_indice)';
COMMENT ON COLUMN analytics.analytics_dashboard_diario.ind_nome_indice IS 'Nome completo do índice (desnormalizado)';
COMMENT ON COLUMN analytics.analytics_dashboard_diario.ind_categoria   IS 'Categoria do índice (desnormalizado)';
COMMENT ON COLUMN analytics.analytics_dashboard_diario.ind_descricao   IS 'Descrição detalhada do índice (desnormalizado)';

-- -----------------------------------------------------------------------------
-- Tabela: analytics.analytics_serie_temporal_ismb
-- Série temporal completa do ISMB com médias móveis e Bandas de Bollinger.

CREATE TABLE IF NOT EXISTS analytics.analytics_serie_temporal_ismb (
    dat_ref            VARCHAR(10)          NOT NULL,
    value              NUMERIC(8,  2),      -- valor do ISMB no dia [0-100]
    daily_return       NUMERIC(10, 2),      -- retorno diário (variação fracional)
    ma_7               NUMERIC(8,  2),      -- média móvel 7 dias
    ma_21              NUMERIC(8,  2),      -- média móvel 21 dias
    bb_mid             NUMERIC(8,  2),      -- Bollinger Band: linha central (ma_21)
    bb_upper           NUMERIC(8,  2),      -- Bollinger Band: banda superior
    bb_lower           NUMERIC(8,  2),      -- Bollinger Band: banda inferior
    rolling_mean       NUMERIC(10, 2),      -- média dos retornos na janela
    rolling_std        NUMERIC(10, 2),      -- desvio padrão dos retornos na janela
    rolling_vol_annual NUMERIC(10, 2),      -- volatilidade anualizada
    ret_zscore         NUMERIC(10, 2),      -- z-score do retorno diário
    CONSTRAINT pk_analytics_serie_temporal_ismb PRIMARY KEY (dat_ref)
);

COMMENT ON TABLE  analytics.analytics_serie_temporal_ismb IS 'Série temporal diária do ISMB com indicadores técnicos: MM, Bollinger Bands, volatilidade.';
COMMENT ON COLUMN analytics.analytics_serie_temporal_ismb.dat_ref            IS 'Data de referência (formato YYYY-MM-DD)';
COMMENT ON COLUMN analytics.analytics_serie_temporal_ismb.value              IS 'Valor do índice ISMB no dia [0-100]';
COMMENT ON COLUMN analytics.analytics_serie_temporal_ismb.daily_return       IS 'Retorno diário do ISMB (variação fracional)';
COMMENT ON COLUMN analytics.analytics_serie_temporal_ismb.ma_7               IS 'Média móvel de 7 dias do ISMB';
COMMENT ON COLUMN analytics.analytics_serie_temporal_ismb.ma_21              IS 'Média móvel de 21 dias do ISMB';
COMMENT ON COLUMN analytics.analytics_serie_temporal_ismb.bb_mid             IS 'Bollinger Band: linha central (igual a ma_21)';
COMMENT ON COLUMN analytics.analytics_serie_temporal_ismb.bb_upper           IS 'Bollinger Band superior: bb_mid + 2x desvio padrão móvel';
COMMENT ON COLUMN analytics.analytics_serie_temporal_ismb.bb_lower           IS 'Bollinger Band inferior: bb_mid - 2x desvio padrão móvel';
COMMENT ON COLUMN analytics.analytics_serie_temporal_ismb.rolling_mean       IS 'Média dos retornos na janela móvel';
COMMENT ON COLUMN analytics.analytics_serie_temporal_ismb.rolling_std        IS 'Desvio padrão dos retornos na janela móvel';
COMMENT ON COLUMN analytics.analytics_serie_temporal_ismb.rolling_vol_annual IS 'Volatilidade anualizada baseada no desvio padrão dos retornos';
COMMENT ON COLUMN analytics.analytics_serie_temporal_ismb.ret_zscore         IS 'Z-score do retorno diário (desvios da média)';

-- -----------------------------------------------------------------------------
-- Tabela: analytics.analytics_correlacao
-- Matriz de correlação entre pares de índices para um período de referência.

CREATE TABLE IF NOT EXISTS analytics.analytics_correlacao (
    idx_a       VARCHAR(16)   NOT NULL,  -- primeiro índice do par
    idx_b       VARCHAR(16)   NOT NULL,  -- segundo índice do par
    corr        NUMERIC(8, 6),           -- coeficiente de correlação de Pearson [-1, 1]
    n_obs       INTEGER,                 -- número de observações no período
    start_date  DATE,                    -- início da janela de correlação
    end_date    DATE,                    -- fim da janela de correlação
    dat_ref     VARCHAR(10)   NOT NULL,  -- data de execução do cálculo
    CONSTRAINT pk_analytics_correlacao   PRIMARY KEY (idx_a, idx_b, dat_ref),
    CONSTRAINT chk_corr_range           CHECK (corr IS NULL OR corr BETWEEN -1 AND 1)
);

COMMENT ON TABLE  analytics.analytics_correlacao IS 'Correlação de Pearson entre pares de índices para uma janela temporal.';
COMMENT ON COLUMN analytics.analytics_correlacao.idx_a      IS 'Primeiro índice do par. Sempre idx_a <= idx_b para evitar duplicatas (A,B)/(B,A)';
COMMENT ON COLUMN analytics.analytics_correlacao.idx_b      IS 'Segundo índice do par';
COMMENT ON COLUMN analytics.analytics_correlacao.corr       IS 'Coeficiente de correlação de Pearson [-1, 1]';
COMMENT ON COLUMN analytics.analytics_correlacao.n_obs      IS 'Número de observações sobrepostas no par (dias com dados em ambos os índices)';
COMMENT ON COLUMN analytics.analytics_correlacao.start_date IS 'Início da janela temporal de correlação';
COMMENT ON COLUMN analytics.analytics_correlacao.end_date   IS 'Fim da janela temporal de correlação';
COMMENT ON COLUMN analytics.analytics_correlacao.dat_ref    IS 'Data de execução do cálculo de correlação';

-- -----------------------------------------------------------------------------
-- Tabela: analytics.analytics_kpis_agregados
-- KPIs anuais agregados por entidade (ISMB ou cada índice individual).

CREATE TABLE IF NOT EXISTS analytics.analytics_kpis_agregados (
    entity_type       VARCHAR(64)   NOT NULL,  -- tipo: 'ISMB', 'INDICE', etc.
    entity_name       VARCHAR(64)   NOT NULL,  -- nome: 'ISMB', 'IBOV', 'CDS', etc.
    year              SMALLINT      NOT NULL,  -- ano de referência
    annual_return     NUMERIC(12, 2),          -- retorno anualizado (fracional)
    annual_volatility NUMERIC(12, 2),          -- volatilidade anualizada
    max_drawdown      NUMERIC(10, 2),          -- drawdown máximo do período [0, -1]
    n_obs             INTEGER,                 -- número de observações no ano
    dat_ref           VARCHAR(10)   NOT NULL,  -- data de execução do cálculo
    CONSTRAINT pk_analytics_kpis_agregados PRIMARY KEY (entity_type, entity_name, year),
    CONSTRAINT chk_kpis_drawdown CHECK (max_drawdown IS NULL OR max_drawdown BETWEEN -1 AND 0)
);

COMMENT ON TABLE  analytics.analytics_kpis_agregados IS 'KPIs anuais consolidados por entidade: retorno, volatilidade e drawdown máximo.';
COMMENT ON COLUMN analytics.analytics_kpis_agregados.entity_type       IS 'Tipo de entidade (ISMB, INDICE, etc.)';
COMMENT ON COLUMN analytics.analytics_kpis_agregados.entity_name       IS 'Nome da entidade (ISMB, IBOV, CDS, IVVB, IFIX)';
COMMENT ON COLUMN analytics.analytics_kpis_agregados.year              IS 'Ano de referência do KPI';
COMMENT ON COLUMN analytics.analytics_kpis_agregados.annual_return     IS 'Retorno acumulado do ano em formato fracional';
COMMENT ON COLUMN analytics.analytics_kpis_agregados.annual_volatility IS 'Volatilidade anualizada do período';
COMMENT ON COLUMN analytics.analytics_kpis_agregados.max_drawdown      IS 'Drawdown máximo do período: valor entre -1 (perda total) e 0 (sem perda)';
COMMENT ON COLUMN analytics.analytics_kpis_agregados.n_obs             IS 'Número de observações (dias) utilizadas no cálculo do ano';
COMMENT ON COLUMN analytics.analytics_kpis_agregados.dat_ref           IS 'Data de execução do cálculo dos KPIs';


-- Índices - schema analytics

-- dashboard_diario
CREATE INDEX IF NOT EXISTS idx_add_dat_desc        ON analytics.analytics_dashboard_diario    (dat_ref DESC);
CREATE INDEX IF NOT EXISTS idx_add_indice_dat      ON analytics.analytics_dashboard_diario    (cod_indice, dat_ref DESC);

-- serie_temporal_ismb
CREATE INDEX IF NOT EXISTS idx_brin_asti_dat       ON analytics.analytics_serie_temporal_ismb USING BRIN (dat_ref);

-- correlacao
CREATE INDEX IF NOT EXISTS idx_ac_par              ON analytics.analytics_correlacao          (idx_a, idx_b, dat_ref DESC);
CREATE INDEX IF NOT EXISTS idx_ac_dat              ON analytics.analytics_correlacao          (dat_ref DESC);

-- kpis_agregados
CREATE INDEX IF NOT EXISTS idx_aka_entity_year     ON analytics.analytics_kpis_agregados      (entity_name, year DESC);
CREATE INDEX IF NOT EXISTS idx_aka_dat             ON analytics.analytics_kpis_agregados      (dat_ref DESC);
