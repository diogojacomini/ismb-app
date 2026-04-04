-- ISMB Database - Schema: stage
-- Dados brutos ingeridos diretamente das fontes externas (séries de mercado
-- e notícias financeiras) antes de qualquer transformação de negócio.
-- Ordem de execução: 02

SET search_path TO stage, public;

-- -----------------------------------------------------------------------------
-- Tabela: stage.stage_cds
-- CDS Brasil 5Y - Credit Default Swap (risco soberano)
-- Fonte: investing.com (scraping HTML)

CREATE TABLE IF NOT EXISTS stage.stage_cds (
    dat_ref           VARCHAR(10) NOT NULL,
    open_price        NUMERIC(12, 2),
    close_price       NUMERIC(12, 2),
    high_price        NUMERIC(12, 2),
    low_price         NUMERIC(12, 2),
    change_percentage NUMERIC(8, 2),
    CONSTRAINT pk_stage_cds PRIMARY KEY (dat_ref)
);

COMMENT ON TABLE  stage.stage_cds IS 'Série histórica diária do CDS Brasil 5Y (risco soberano), ingerida via scraping.';
COMMENT ON COLUMN stage.stage_cds.dat_ref           IS 'Data de referência';
COMMENT ON COLUMN stage.stage_cds.open_price        IS 'Preço de abertura do dia';
COMMENT ON COLUMN stage.stage_cds.close_price       IS 'Preço de fechamento do dia';
COMMENT ON COLUMN stage.stage_cds.high_price        IS 'Preço máximo atingido no dia';
COMMENT ON COLUMN stage.stage_cds.low_price         IS 'Preço mínimo atingido no dia';
COMMENT ON COLUMN stage.stage_cds.change_percentage IS 'Variação percentual diária em relação ao dia anterior';

-- -----------------------------------------------------------------------------
-- Tabela: stage.stage_ibov
-- Ibovespa - principal índice da B3
-- Fonte: Yahoo Finance (yfinance, ticker ^BVSP)

CREATE TABLE IF NOT EXISTS stage.stage_ibov (
    dat_ref         VARCHAR(10) NOT NULL,
    close_adj_price NUMERIC(14, 2),
    close_price     NUMERIC(14, 2),
    high_price      NUMERIC(14, 2),
    low_price       NUMERIC(14, 2),
    open_price      NUMERIC(14, 2),
    volume          BIGINT,
    CONSTRAINT pk_stage_ibov PRIMARY KEY (dat_ref)
);

COMMENT ON TABLE  stage.stage_ibov IS 'Série histórica diária do Ibovespa (^BVSP) via Yahoo Finance.';
COMMENT ON COLUMN stage.stage_ibov.dat_ref         IS 'Data de referência';
COMMENT ON COLUMN stage.stage_ibov.close_adj_price IS 'Preço de fechamento ajustado por proventos e splits';
COMMENT ON COLUMN stage.stage_ibov.close_price     IS 'Preço de fechamento do dia';
COMMENT ON COLUMN stage.stage_ibov.high_price      IS 'Preço máximo atingido no dia';
COMMENT ON COLUMN stage.stage_ibov.low_price       IS 'Preço mínimo atingido no dia';
COMMENT ON COLUMN stage.stage_ibov.open_price      IS 'Preço de abertura do dia';
COMMENT ON COLUMN stage.stage_ibov.volume          IS 'Volume negociado no dia';

-- -----------------------------------------------------------------------------
-- Tabela: stage.stage_ivvb
-- IVVB11 - ETF que replica o VIX Brasil (volatilidade implícita)
-- Fonte: Yahoo Finance (yfinance, ticker IVVB11.SA)

CREATE TABLE IF NOT EXISTS stage.stage_ivvb (
    dat_ref         VARCHAR(10) NOT NULL,
    close_adj_price NUMERIC(12, 2),
    close_price     NUMERIC(12, 2),
    high_price      NUMERIC(12, 2),
    low_price       NUMERIC(12, 2),
    open_price      NUMERIC(12, 2),
    volume          BIGINT,
    CONSTRAINT pk_stage_ivvb PRIMARY KEY (dat_ref)
);

COMMENT ON TABLE  stage.stage_ivvb IS 'Série histórica diária do IVVB11 (VIX Brasil) via Yahoo Finance.';
COMMENT ON COLUMN stage.stage_ivvb.dat_ref         IS 'Data de referência';
COMMENT ON COLUMN stage.stage_ivvb.close_adj_price IS 'Preço de fechamento ajustado por proventos e splits';
COMMENT ON COLUMN stage.stage_ivvb.close_price     IS 'Preço de fechamento do dia';
COMMENT ON COLUMN stage.stage_ivvb.high_price      IS 'Preço máximo atingido no dia';
COMMENT ON COLUMN stage.stage_ivvb.low_price       IS 'Preço mínimo atingido no dia';
COMMENT ON COLUMN stage.stage_ivvb.open_price      IS 'Preço de abertura do dia';
COMMENT ON COLUMN stage.stage_ivvb.volume          IS 'Volume negociado no dia';

-- -----------------------------------------------------------------------------
-- Tabela: stage.stage_ifix
-- IFIX - Índice de Fundos de Investimento Imobiliário da B3
-- Fonte: investing.com (scraping HTML)

CREATE TABLE IF NOT EXISTS stage.stage_ifix (
    dat_ref           VARCHAR(10) NOT NULL,
    open_price        NUMERIC(12, 2),
    close_price       NUMERIC(12, 2),
    high_price        NUMERIC(12, 2),
    low_price         NUMERIC(12, 2),
    change_percentage NUMERIC(8, 2),
    CONSTRAINT pk_stage_ifix PRIMARY KEY (dat_ref)
);

COMMENT ON TABLE  stage.stage_ifix IS 'Série histórica diária do IFIX (fundos imobiliários) via scraping.';
COMMENT ON COLUMN stage.stage_ifix.dat_ref           IS 'Data de referência';
COMMENT ON COLUMN stage.stage_ifix.open_price        IS 'Preço de abertura do dia';
COMMENT ON COLUMN stage.stage_ifix.close_price       IS 'Preço de fechamento do dia';
COMMENT ON COLUMN stage.stage_ifix.high_price        IS 'Preço máximo atingido no dia';
COMMENT ON COLUMN stage.stage_ifix.low_price         IS 'Preço mínimo atingido no dia';
COMMENT ON COLUMN stage.stage_ifix.change_percentage IS 'Variação percentual diária em relação ao dia anterior';

-- -----------------------------------------------------------------------------
-- Tabela: stage.stage_infomoney
-- Notícias capturadas do portal InfoMoney

CREATE TABLE IF NOT EXISTS stage.stage_infomoney (
    id_news  VARCHAR(32)  NOT NULL,
    dat_ref  VARCHAR(10)  NOT NULL,
    fonte    VARCHAR(64)  NOT NULL DEFAULT 'InfoMoney',
    titulo   TEXT         NOT NULL,
    link     TEXT,
    CONSTRAINT pk_stage_infomoney PRIMARY KEY (id_news)
);

COMMENT ON TABLE  stage.stage_infomoney IS 'Notícias financeiras coletadas via scraping do portal InfoMoney.';
COMMENT ON COLUMN stage.stage_infomoney.id_news IS 'Hash identificador único da notícia (ismb_<sha256_prefix>)';
COMMENT ON COLUMN stage.stage_infomoney.dat_ref IS 'Data de publicação da notícia';
COMMENT ON COLUMN stage.stage_infomoney.fonte   IS 'Nome do portal fonte (InfoMoney)';
COMMENT ON COLUMN stage.stage_infomoney.titulo  IS 'Título completo da notícia';
COMMENT ON COLUMN stage.stage_infomoney.link    IS 'URL completa da notícia no portal';

-- -----------------------------------------------------------------------------
-- Tabela: stage.stage_valorinveste
-- Notícias capturadas do portal Valor Investe (Globo)

CREATE TABLE IF NOT EXISTS stage.stage_valorinveste (
    id_news  VARCHAR(32)  NOT NULL,
    dat_ref  VARCHAR(10)  NOT NULL,
    fonte    VARCHAR(64)  NOT NULL DEFAULT 'Valor Investe',
    titulo   TEXT         NOT NULL,
    link     TEXT,
    CONSTRAINT pk_stage_valorinveste PRIMARY KEY (id_news)
);

COMMENT ON TABLE  stage.stage_valorinveste IS 'Notícias financeiras coletadas via scraping do portal Valor Investe.';
COMMENT ON COLUMN stage.stage_valorinveste.id_news IS 'Hash identificador único da notícia (ismb_<sha256_prefix>)';
COMMENT ON COLUMN stage.stage_valorinveste.dat_ref IS 'Data de publicação da notícia';
COMMENT ON COLUMN stage.stage_valorinveste.fonte   IS 'Nome do portal fonte (Valor Investe)';
COMMENT ON COLUMN stage.stage_valorinveste.titulo  IS 'Título completo da notícia';
COMMENT ON COLUMN stage.stage_valorinveste.link    IS 'URL completa da notícia no portal';

-- -----------------------------------------------------------------------------
-- Tabela: stage.stage_seudinheiro
-- Notícias capturadas do portal Seu Dinheiro

CREATE TABLE IF NOT EXISTS stage.stage_seudinheiro (
    id_news  VARCHAR(32)  NOT NULL,
    dat_ref  VARCHAR(10)  NOT NULL,
    fonte    VARCHAR(64)  NOT NULL DEFAULT 'Seu Dinheiro',
    titulo   TEXT         NOT NULL,
    link     TEXT,
    CONSTRAINT pk_stage_seudinheiro PRIMARY KEY (id_news)
);

COMMENT ON TABLE  stage.stage_seudinheiro IS 'Notícias financeiras coletadas via scraping do portal Seu Dinheiro.';
COMMENT ON COLUMN stage.stage_seudinheiro.id_news IS 'Hash identificador único da notícia (ismb_<sha256_prefix>)';
COMMENT ON COLUMN stage.stage_seudinheiro.dat_ref IS 'Data de publicação da notícia';
COMMENT ON COLUMN stage.stage_seudinheiro.fonte   IS 'Nome do portal fonte (Seu Dinheiro)';
COMMENT ON COLUMN stage.stage_seudinheiro.titulo  IS 'Título completo da notícia';
COMMENT ON COLUMN stage.stage_seudinheiro.link    IS 'URL completa da notícia no portal';

-- -----------------------------------------------------------------------------
-- Tabela: stage.stage_moneytimes
-- Notícias capturadas do portal Money Times

CREATE TABLE IF NOT EXISTS stage.stage_moneytimes (
    id_news  VARCHAR(32)  NOT NULL,
    dat_ref  VARCHAR(10)  NOT NULL,
    fonte    VARCHAR(64)  NOT NULL DEFAULT 'MoneyTimes',
    titulo   TEXT         NOT NULL,
    link     TEXT,
    CONSTRAINT pk_stage_moneytimes PRIMARY KEY (id_news)
);

COMMENT ON TABLE  stage.stage_moneytimes IS 'Notícias financeiras coletadas via scraping do portal Money Times.';
COMMENT ON COLUMN stage.stage_moneytimes.id_news IS 'Hash identificador único da notícia (ismb_<sha256_prefix>)';
COMMENT ON COLUMN stage.stage_moneytimes.dat_ref IS 'Data de publicação da notícia';
COMMENT ON COLUMN stage.stage_moneytimes.fonte   IS 'Nome do portal fonte (MoneyTimes)';
COMMENT ON COLUMN stage.stage_moneytimes.titulo  IS 'Título completo da notícia';
COMMENT ON COLUMN stage.stage_moneytimes.link    IS 'URL completa da notícia no portal';

-- =============================================================================
-- Índices - schema stage
-- Estratégia: BRIN para colunas de data em séries temporais (alta cardinalidade, inserção ordenada).
-- (BRIN ocupa ~100x menos espaço que B-tree em séries temporais ordenadas)

-- Mercado
CREATE INDEX IF NOT EXISTS idx_brin_stage_cds_dat   ON stage.stage_cds   USING BRIN (dat_ref);
CREATE INDEX IF NOT EXISTS idx_brin_stage_ibov_dat  ON stage.stage_ibov  USING BRIN (dat_ref);
CREATE INDEX IF NOT EXISTS idx_brin_stage_ivvb_dat  ON stage.stage_ivvb  USING BRIN (dat_ref);
CREATE INDEX IF NOT EXISTS idx_brin_stage_ifix_dat  ON stage.stage_ifix  USING BRIN (dat_ref);

-- Notícias
CREATE INDEX IF NOT EXISTS idx_stage_infomoney_dat    ON stage.stage_infomoney    (dat_ref DESC);
CREATE INDEX IF NOT EXISTS idx_stage_valorinveste_dat ON stage.stage_valorinveste (dat_ref DESC);
CREATE INDEX IF NOT EXISTS idx_stage_seudinheiro_dat  ON stage.stage_seudinheiro  (dat_ref DESC);
CREATE INDEX IF NOT EXISTS idx_stage_moneytimes_dat   ON stage.stage_moneytimes   (dat_ref DESC);

-- Índice GIN de trigram para busca textual no título das notícias
CREATE INDEX IF NOT EXISTS idx_trgm_infomoney_titulo    ON stage.stage_infomoney    USING GIN (titulo gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_trgm_valorinveste_titulo ON stage.stage_valorinveste USING GIN (titulo gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_trgm_seudinheiro_titulo  ON stage.stage_seudinheiro  USING GIN (titulo gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_trgm_moneytimes_titulo   ON stage.stage_moneytimes   USING GIN (titulo gin_trgm_ops);
