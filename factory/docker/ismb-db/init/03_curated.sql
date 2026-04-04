-- ISMB Database - Schema: curated
-- Data Warehouse: dimensões e tabelas fatos consolidados.
-- Ordem de execução: 03

SET search_path TO curated, public;

-- DIMENSÕES

-- -----------------------------------------------------------------------------
-- Tabela: curated.dim_indice
-- Cadastro dos índices financeiros monitorados pelo ISMB

CREATE TABLE IF NOT EXISTS curated.dim_indice (
    sk_indice   SMALLINT     NOT NULL,
    cod_indice  VARCHAR(16)  NOT NULL,
    nome_indice VARCHAR(128) NOT NULL,
    categoria   VARCHAR(64),
    descricao   TEXT,
    CONSTRAINT pk_dim_indice        PRIMARY KEY (sk_indice),
    CONSTRAINT uq_dim_indice_cod    UNIQUE (cod_indice)
);

COMMENT ON TABLE  curated.dim_indice IS 'Dimensão dos índices financeiros monitorados (CDS, IBOV, IVVB, IFIX).';
COMMENT ON COLUMN curated.dim_indice.sk_indice   IS 'Surrogate key do índice';
COMMENT ON COLUMN curated.dim_indice.cod_indice  IS 'Código curto do índice (ex: IBOV, CDS, IVVB, IFIX)';
COMMENT ON COLUMN curated.dim_indice.nome_indice IS 'Nome completo do índice';
COMMENT ON COLUMN curated.dim_indice.categoria   IS 'Categoria de mercado do índice (ex: Ações, Renda Fixa, Volatilidade)';
COMMENT ON COLUMN curated.dim_indice.descricao   IS 'Descrição detalhada do que o índice representa';

-- -----------------------------------------------------------------------------
-- Tabela: curated.dim_tempo
-- Cada linha é um dia, com atributos de data pré-calculados.

CREATE TABLE IF NOT EXISTS curated.dim_tempo (
    sk_tempo         INTEGER      NOT NULL,   -- YYYYMMDD
    dat_ref          VARCHAR(10)  NOT NULL,
    ano              SMALLINT     NOT NULL,
    mes              SMALLINT     NOT NULL CHECK (mes BETWEEN 1 AND 12),
    dia_mes          SMALLINT     NOT NULL CHECK (dia_mes BETWEEN 1 AND 31),
    nome_mes         VARCHAR(16)  NOT NULL,
    nome_dia_semana  VARCHAR(16)  NOT NULL,
    dia_semana       SMALLINT     NOT NULL CHECK (dia_semana BETWEEN 0 AND 6),
    trimestre        SMALLINT     NOT NULL CHECK (trimestre BETWEEN 1 AND 4),
    semana_ano       SMALLINT     NOT NULL CHECK (semana_ano BETWEEN 1 AND 53),
    dia_util         SMALLINT     NOT NULL DEFAULT 0,
    feriado          SMALLINT     NOT NULL DEFAULT 0,
    nome_feriado     VARCHAR(64),
    fim_semana       SMALLINT     NOT NULL DEFAULT 0,
    CONSTRAINT pk_dim_tempo       PRIMARY KEY (sk_tempo),
    CONSTRAINT uq_dim_tempo_dat   UNIQUE (dat_ref)
);

COMMENT ON TABLE  curated.dim_tempo IS 'Dimensão de tempo (date spine) com atributos de calendário pré-calculados.';
COMMENT ON COLUMN curated.dim_tempo.sk_tempo         IS 'Surrogate key no formato YYYYMMDD (ex: 20260330)';
COMMENT ON COLUMN curated.dim_tempo.dat_ref          IS 'Data de referência no formato YYYY-MM-DD';
COMMENT ON COLUMN curated.dim_tempo.ano              IS 'Ano (ex: 2026)';
COMMENT ON COLUMN curated.dim_tempo.mes              IS 'Mês (1-12)';
COMMENT ON COLUMN curated.dim_tempo.dia_mes          IS 'Dia do mês (1-31)';
COMMENT ON COLUMN curated.dim_tempo.nome_mes         IS 'Nome do mês (ex: Janeiro, Fevereiro)';
COMMENT ON COLUMN curated.dim_tempo.nome_dia_semana  IS 'Nome do dia da semana (ex: Segunda, Terça)';
COMMENT ON COLUMN curated.dim_tempo.dia_semana       IS 'Dia da semana (0=Segunda, 1=Terça, ..., 6=Domingo)';
COMMENT ON COLUMN curated.dim_tempo.trimestre        IS 'Trimestre do ano (1-4)';
COMMENT ON COLUMN curated.dim_tempo.semana_ano       IS 'Número da semana no ano ISO (1-53)';
COMMENT ON COLUMN curated.dim_tempo.dia_util         IS 'Indica se é dia útil (0=Não, 1=Sim)';
COMMENT ON COLUMN curated.dim_tempo.feriado          IS 'Indica se é feriado nacional (0=Não, 1=Sim)';
COMMENT ON COLUMN curated.dim_tempo.nome_feriado     IS 'Nome do feriado (quando aplicável)';
COMMENT ON COLUMN curated.dim_tempo.fim_semana       IS 'Indica se é fim de semana (0=Não, 1=Sim)';

-- -----------------------------------------------------------------------------
-- Tabela: curated.dim_indicador
-- Cadastro dos indicadores que compõem o índice ISMB

CREATE TABLE IF NOT EXISTS curated.dim_indicador (
    sk_indicador   SMALLINT     NOT NULL,
    cod_indicador  VARCHAR(32)  NOT NULL,
    nome_indicador VARCHAR(128) NOT NULL,
    descricao      TEXT,
    peso_ismb      NUMERIC(4, 2) NOT NULL CHECK (peso_ismb BETWEEN 0 AND 1),
    CONSTRAINT pk_dim_indicador     PRIMARY KEY (sk_indicador),
    CONSTRAINT uq_dim_indicador_cod UNIQUE      (cod_indicador)
);

COMMENT ON TABLE  curated.dim_indicador IS 'Dimensão dos indicadores que compõem o índice ISMB (6 componentes).';
COMMENT ON COLUMN curated.dim_indicador.sk_indicador   IS 'Surrogate key do indicador';
COMMENT ON COLUMN curated.dim_indicador.cod_indicador  IS 'Código do indicador (ex: RISCO_CREDITO, SENTIMENTO_NOTICIAS)';
COMMENT ON COLUMN curated.dim_indicador.nome_indicador IS 'Nome completo do indicador';
COMMENT ON COLUMN curated.dim_indicador.descricao      IS 'Descrição detalhada da métrica e metodologia de cálculo';
COMMENT ON COLUMN curated.dim_indicador.peso_ismb      IS 'Peso do indicador no cálculo do índice ISMB (soma dos pesos = 1.00)';

-- -----------------------------------------------------------------------------
-- Tabela: curated.dim_fonte_noticia
-- Cadastro das fontes de notícias

CREATE TABLE IF NOT EXISTS curated.dim_fonte_noticia (
    sk_fonte       SMALLINT     NOT NULL,
    cod_fonte      VARCHAR(32)  NOT NULL,
    nome_fonte     VARCHAR(128) NOT NULL,
    url            VARCHAR(256),
    confiabilidade VARCHAR(16)  CHECK (confiabilidade IN ('Alta', 'Media', 'Baixa')),
    CONSTRAINT pk_dim_fonte_noticia     PRIMARY KEY (sk_fonte),
    CONSTRAINT uq_dim_fonte_noticia_cod UNIQUE      (cod_fonte)
);

COMMENT ON TABLE  curated.dim_fonte_noticia IS 'Dimensão das fontes de notícias financeiras monitoradas (InfoMoney, Valor Investe, etc.).';
COMMENT ON COLUMN curated.dim_fonte_noticia.sk_fonte       IS 'Surrogate key da fonte';
COMMENT ON COLUMN curated.dim_fonte_noticia.cod_fonte      IS 'Código da fonte (ex: INFOMONEY, VALORINVESTE)';
COMMENT ON COLUMN curated.dim_fonte_noticia.nome_fonte     IS 'Nome completo do portal de notícias';
COMMENT ON COLUMN curated.dim_fonte_noticia.url            IS 'URL base do portal';
COMMENT ON COLUMN curated.dim_fonte_noticia.confiabilidade IS 'Grau de confiabilidade editorial: Alta | Media | Baixa';


-- FATOS

-- -----------------------------------------------------------------------------
-- Tabela: curated.fato_transacao_mercado
-- Série histórica diária consolidada de todos os índices de mercado

CREATE TABLE IF NOT EXISTS curated.fato_transacao_mercado (
    dat_ref        VARCHAR(10)  NOT NULL,
    cod_indice     VARCHAR(16)  NOT NULL,
    val_fechamento NUMERIC(14, 2),
    val_abertura   NUMERIC(14, 2),
    val_maxima     NUMERIC(14, 2),
    val_minima     NUMERIC(14, 2),
    qtd_volume     NUMERIC(20, 2),
    CONSTRAINT pk_fato_transacao_mercado PRIMARY KEY (dat_ref, cod_indice),
    CONSTRAINT fk_fato_mercado_indice
        FOREIGN KEY (cod_indice) REFERENCES curated.dim_indice (cod_indice)
        ON UPDATE CASCADE ON DELETE RESTRICT
);

COMMENT ON TABLE  curated.fato_transacao_mercado IS 'Fato de transação diária de mercado: OHLCV consolidado de todos os índices.';
COMMENT ON COLUMN curated.fato_transacao_mercado.dat_ref        IS 'Data de referência da transação (formato YYYY-MM-DD)';
COMMENT ON COLUMN curated.fato_transacao_mercado.cod_indice     IS 'Código do índice (chave estrangeira para dim_indice)';
COMMENT ON COLUMN curated.fato_transacao_mercado.val_fechamento IS 'Preço de fechamento do dia';
COMMENT ON COLUMN curated.fato_transacao_mercado.val_abertura   IS 'Preço de abertura do dia';
COMMENT ON COLUMN curated.fato_transacao_mercado.val_maxima     IS 'Preço máximo atingido no dia';
COMMENT ON COLUMN curated.fato_transacao_mercado.val_minima     IS 'Preço mínimo atingido no dia';
COMMENT ON COLUMN curated.fato_transacao_mercado.qtd_volume     IS 'Volume negociado (NULL para índices que não reportam volume como CDS e IFIX)';

-- -----------------------------------------------------------------------------
-- Tabela: curated.fato_transacao_noticias
-- Registro de cada notícia coletada e consolidada das quatro fontes.

CREATE TABLE IF NOT EXISTS curated.fato_transacao_noticias (
    id_noticia VARCHAR(32)  NOT NULL,
    dat_ref    VARCHAR(10)  NOT NULL,
    cod_fonte  VARCHAR(32)  NOT NULL,
    txt_titulo TEXT         NOT NULL,
    CONSTRAINT pk_fato_noticias  PRIMARY KEY (id_noticia),
    CONSTRAINT uq_fato_noticias  UNIQUE      (dat_ref, cod_fonte, txt_titulo),
    CONSTRAINT fk_fato_noticias_fonte
        FOREIGN KEY (cod_fonte) REFERENCES curated.dim_fonte_noticia (cod_fonte)
        ON UPDATE CASCADE ON DELETE RESTRICT
);

COMMENT ON TABLE  curated.fato_transacao_noticias IS 'Fato de notícias financeiras coletadas e consolidadas das quatro fontes.';
COMMENT ON COLUMN curated.fato_transacao_noticias.id_noticia IS 'Identificador único da notícia (hash gerado pelo pipeline)';
COMMENT ON COLUMN curated.fato_transacao_noticias.dat_ref    IS 'Data de publicação da notícia (formato YYYY-MM-DD)';
COMMENT ON COLUMN curated.fato_transacao_noticias.cod_fonte  IS 'Código da fonte (chave estrangeira para dim_fonte_noticia)';
COMMENT ON COLUMN curated.fato_transacao_noticias.txt_titulo IS 'Título completo da notícia';

-- -----------------------------------------------------------------------------
-- Tabela: curated.fato_indice_ismb
-- Série histórica do Índice ISMB calculado e dos scores de cada componente

CREATE TABLE IF NOT EXISTS curated.fato_indice_ismb (
    dat_ref                    VARCHAR(10) NOT NULL,
    score_risco_credito        NUMERIC(8, 2),
    score_retorno_mercado      NUMERIC(8, 2),
    score_volatilidade_mercado NUMERIC(8, 2),
    score_atividade_mercado    NUMERIC(8, 2),
    score_confianca_mercado    NUMERIC(8, 2),
    score_noticias             NUMERIC(8, 2),
    indice_ismb                NUMERIC(8, 2),
    CONSTRAINT pk_fato_indice_ismb PRIMARY KEY (dat_ref),
    CONSTRAINT chk_indice_ismb_range
        CHECK (indice_ismb IS NULL OR indice_ismb BETWEEN 0 AND 100)
);

COMMENT ON TABLE  curated.fato_indice_ismb IS 'Série histórica do Índice ISMB e dos seis scores componentes (0-100).';
COMMENT ON COLUMN curated.fato_indice_ismb.dat_ref                    IS 'Data de referência do cálculo (formato YYYY-MM-DD)';
COMMENT ON COLUMN curated.fato_indice_ismb.score_risco_credito        IS 'Score do indicador de risco de crédito (0-100)';
COMMENT ON COLUMN curated.fato_indice_ismb.score_retorno_mercado      IS 'Score do indicador de retorno do mercado (0-100)';
COMMENT ON COLUMN curated.fato_indice_ismb.score_volatilidade_mercado IS 'Score do indicador de volatilidade do mercado (0-100)';
COMMENT ON COLUMN curated.fato_indice_ismb.score_atividade_mercado    IS 'Score do indicador de atividade do mercado (0-100)';
COMMENT ON COLUMN curated.fato_indice_ismb.score_confianca_mercado    IS 'Score do indicador de confiança no mercado local (0-100)';
COMMENT ON COLUMN curated.fato_indice_ismb.score_noticias             IS 'Score do indicador de sentimento de notícias (0-100)';
COMMENT ON COLUMN curated.fato_indice_ismb.indice_ismb                IS 'Valor composto final do ISMB (média ponderada dos scores, escala 0-100)';


-- Índices - schema curated

-- dim_tempo
CREATE INDEX IF NOT EXISTS idx_dim_tempo_ano_mes ON curated.dim_tempo (ano, mes);
CREATE INDEX IF NOT EXISTS idx_dim_tempo_trimestre ON curated.dim_tempo (ano, trimestre);
CREATE INDEX IF NOT EXISTS idx_dim_tempo_dia_util ON curated.dim_tempo (dat_ref) WHERE dia_util = 1;

-- fato_transacao_mercado
CREATE INDEX IF NOT EXISTS idx_brin_ftm_dat ON curated.fato_transacao_mercado USING BRIN (dat_ref);
CREATE INDEX IF NOT EXISTS idx_ftm_cod_indice ON curated.fato_transacao_mercado (cod_indice);
CREATE INDEX IF NOT EXISTS idx_ftm_indice_dat ON curated.fato_transacao_mercado (cod_indice, dat_ref DESC);

-- fato_indice_ismb
CREATE INDEX IF NOT EXISTS idx_brin_fii_dat ON curated.fato_indice_ismb USING BRIN (dat_ref);

-- fato_transacao_noticias
CREATE INDEX IF NOT EXISTS idx_ftn_dat_fonte ON curated.fato_transacao_noticias (dat_ref DESC, cod_fonte);
CREATE INDEX IF NOT EXISTS idx_trgm_ftn_titulo ON curated.fato_transacao_noticias USING GIN (txt_titulo gin_trgm_ops);
