-- ISMB Database - Schema: sandbox
-- Ambiente de teste/validação que espelha toda a estrutura de produção em

CREATE SCHEMA IF NOT EXISTS sandbox;

COMMENT ON SCHEMA sandbox IS
    'Ambiente de teste/sandbox: espelha toda a estrutura de produção em um único schema. '
    'Sem FK constraints - ideal para testes isolados e pipelines de desenvolvimento.';


-- Schema: stage

CREATE TABLE IF NOT EXISTS sandbox.stage_cds          (LIKE stage.stage_cds          INCLUDING ALL);
CREATE TABLE IF NOT EXISTS sandbox.stage_ibov         (LIKE stage.stage_ibov         INCLUDING ALL);
CREATE TABLE IF NOT EXISTS sandbox.stage_ivvb         (LIKE stage.stage_ivvb         INCLUDING ALL);
CREATE TABLE IF NOT EXISTS sandbox.stage_ifix         (LIKE stage.stage_ifix         INCLUDING ALL);
CREATE TABLE IF NOT EXISTS sandbox.stage_infomoney    (LIKE stage.stage_infomoney    INCLUDING ALL);
CREATE TABLE IF NOT EXISTS sandbox.stage_valorinveste (LIKE stage.stage_valorinveste INCLUDING ALL);
CREATE TABLE IF NOT EXISTS sandbox.stage_seudinheiro  (LIKE stage.stage_seudinheiro  INCLUDING ALL);
CREATE TABLE IF NOT EXISTS sandbox.stage_moneytimes   (LIKE stage.stage_moneytimes   INCLUDING ALL);


-- Schema: curated - dimensões

CREATE TABLE IF NOT EXISTS sandbox.dim_indice        (LIKE curated.dim_indice        INCLUDING ALL);
CREATE TABLE IF NOT EXISTS sandbox.dim_tempo         (LIKE curated.dim_tempo         INCLUDING ALL);
CREATE TABLE IF NOT EXISTS sandbox.dim_indicador     (LIKE curated.dim_indicador     INCLUDING ALL);
CREATE TABLE IF NOT EXISTS sandbox.dim_fonte_noticia (LIKE curated.dim_fonte_noticia INCLUDING ALL);


-- Seed das dimensões estáticas no sandbox
INSERT INTO sandbox.dim_indice (sk_indice, cod_indice, nome_indice, categoria, descricao)
SELECT sk_indice, cod_indice, nome_indice, categoria, descricao
FROM curated.dim_indice
ON CONFLICT (sk_indice) DO NOTHING;

INSERT INTO sandbox.dim_indicador (sk_indicador, cod_indicador, nome_indicador, descricao, peso_ismb)
SELECT sk_indicador, cod_indicador, nome_indicador, descricao, peso_ismb
FROM curated.dim_indicador
ON CONFLICT (sk_indicador) DO NOTHING;

INSERT INTO sandbox.dim_fonte_noticia (sk_fonte, cod_fonte, nome_fonte, url, confiabilidade)
SELECT sk_fonte, cod_fonte, nome_fonte, url, confiabilidade
FROM curated.dim_fonte_noticia
ON CONFLICT (sk_fonte) DO NOTHING;


-- dim_tempo: popula apenas um range de teste (2 anos)
-- O procedimento proc_refresh_from_prod pode substituir este range a qualquer momento.
INSERT INTO sandbox.dim_tempo
SELECT * FROM curated.dim_tempo
WHERE dat_ref::DATE BETWEEN CURRENT_DATE - INTERVAL '1 year'
                        AND CURRENT_DATE + INTERVAL '1 year'
ON CONFLICT (sk_tempo) DO NOTHING;


-- Schema: curated - fatos

CREATE TABLE IF NOT EXISTS sandbox.fato_transacao_mercado  (LIKE curated.fato_transacao_mercado  INCLUDING ALL);
CREATE TABLE IF NOT EXISTS sandbox.fato_transacao_noticias (LIKE curated.fato_transacao_noticias INCLUDING ALL);
CREATE TABLE IF NOT EXISTS sandbox.fato_indice_ismb        (LIKE curated.fato_indice_ismb        INCLUDING ALL);


-- Schema: indicators

CREATE TABLE IF NOT EXISTS sandbox.indicador_risco_credito          (LIKE indicators.indicador_risco_credito          INCLUDING ALL);
CREATE TABLE IF NOT EXISTS sandbox.indicador_retorno_mercado        (LIKE indicators.indicador_retorno_mercado        INCLUDING ALL);
CREATE TABLE IF NOT EXISTS sandbox.indicador_volatilidade_mercado   (LIKE indicators.indicador_volatilidade_mercado   INCLUDING ALL);
CREATE TABLE IF NOT EXISTS sandbox.indicador_atividade_mercado      (LIKE indicators.indicador_atividade_mercado      INCLUDING ALL);
CREATE TABLE IF NOT EXISTS sandbox.indicador_confianca_mercado_local(LIKE indicators.indicador_confianca_mercado_local INCLUDING ALL);
CREATE TABLE IF NOT EXISTS sandbox.indicador_sentimento_noticias    (LIKE indicators.indicador_sentimento_noticias    INCLUDING ALL);


-- Schema: analytics

CREATE TABLE IF NOT EXISTS sandbox.analytics_dashboard_diario    (LIKE analytics.analytics_dashboard_diario    INCLUDING ALL);
CREATE TABLE IF NOT EXISTS sandbox.analytics_serie_temporal_ismb (LIKE analytics.analytics_serie_temporal_ismb INCLUDING ALL);
CREATE TABLE IF NOT EXISTS sandbox.analytics_correlacao          (LIKE analytics.analytics_correlacao          INCLUDING ALL);
CREATE TABLE IF NOT EXISTS sandbox.analytics_kpis_agregados      (LIKE analytics.analytics_kpis_agregados      INCLUDING ALL);


-- Schema: governance

CREATE TABLE IF NOT EXISTS sandbox.pipeline_logs                (LIKE governance.pipeline_logs                INCLUDING ALL);
CREATE TABLE IF NOT EXISTS sandbox.validation_data_mercado      (LIKE governance.validation_data_mercado      INCLUDING ALL);
CREATE TABLE IF NOT EXISTS sandbox.validation_data_indicadores  (LIKE governance.validation_data_indicadores  INCLUDING ALL);
CREATE TABLE IF NOT EXISTS sandbox.validation_indice_isbm       (LIKE governance.validation_indice_isbm       INCLUDING ALL);
CREATE TABLE IF NOT EXISTS sandbox.metrics_stage_cds            (LIKE governance.metrics_stage_cds            INCLUDING ALL);
CREATE TABLE IF NOT EXISTS sandbox.metrics_stage_ibov           (LIKE governance.metrics_stage_ibov           INCLUDING ALL);
CREATE TABLE IF NOT EXISTS sandbox.metrics_stage_ivvb           (LIKE governance.metrics_stage_ivvb           INCLUDING ALL);
CREATE TABLE IF NOT EXISTS sandbox.metrics_stage_ifix           (LIKE governance.metrics_stage_ifix           INCLUDING ALL);
CREATE TABLE IF NOT EXISTS sandbox.metrics_stage_infomoney      (LIKE governance.metrics_stage_infomoney      INCLUDING ALL);
CREATE TABLE IF NOT EXISTS sandbox.metrics_stage_valorinveste   (LIKE governance.metrics_stage_valorinveste   INCLUDING ALL);
CREATE TABLE IF NOT EXISTS sandbox.metrics_stage_seudinheiro    (LIKE governance.metrics_stage_seudinheiro    INCLUDING ALL);
CREATE TABLE IF NOT EXISTS sandbox.metrics_stage_moneytimes     (LIKE governance.metrics_stage_moneytimes     INCLUDING ALL);
CREATE TABLE IF NOT EXISTS sandbox.data_quality_report          (LIKE governance.data_quality_report          INCLUDING ALL);


-- Comentários do schema sandbox

COMMENT ON TABLE sandbox.stage_cds                          IS '[sandbox] Espelho de stage.stage_cds';
COMMENT ON TABLE sandbox.stage_ibov                         IS '[sandbox] Espelho de stage.stage_ibov';
COMMENT ON TABLE sandbox.stage_ivvb                         IS '[sandbox] Espelho de stage.stage_ivvb';
COMMENT ON TABLE sandbox.stage_ifix                         IS '[sandbox] Espelho de stage.stage_ifix';
COMMENT ON TABLE sandbox.stage_infomoney                    IS '[sandbox] Espelho de stage.stage_infomoney';
COMMENT ON TABLE sandbox.stage_valorinveste                 IS '[sandbox] Espelho de stage.stage_valorinveste';
COMMENT ON TABLE sandbox.stage_seudinheiro                  IS '[sandbox] Espelho de stage.stage_seudinheiro';
COMMENT ON TABLE sandbox.stage_moneytimes                   IS '[sandbox] Espelho de stage.stage_moneytimes';
COMMENT ON TABLE sandbox.dim_indice                         IS '[sandbox] Espelho de curated.dim_indice';
COMMENT ON TABLE sandbox.dim_tempo                          IS '[sandbox] Espelho de curated.dim_tempo (range limitado por padrão)';
COMMENT ON TABLE sandbox.dim_indicador                      IS '[sandbox] Espelho de curated.dim_indicador';
COMMENT ON TABLE sandbox.dim_fonte_noticia                  IS '[sandbox] Espelho de curated.dim_fonte_noticia';
COMMENT ON TABLE sandbox.fato_transacao_mercado             IS '[sandbox] Espelho de curated.fato_transacao_mercado';
COMMENT ON TABLE sandbox.fato_transacao_noticias            IS '[sandbox] Espelho de curated.fato_transacao_noticias';
COMMENT ON TABLE sandbox.fato_indice_ismb                   IS '[sandbox] Espelho de curated.fato_indice_ismb';
COMMENT ON TABLE sandbox.indicador_risco_credito            IS '[sandbox] Espelho de indicators.indicador_risco_credito';
COMMENT ON TABLE sandbox.indicador_retorno_mercado          IS '[sandbox] Espelho de indicators.indicador_retorno_mercado';
COMMENT ON TABLE sandbox.indicador_volatilidade_mercado     IS '[sandbox] Espelho de indicators.indicador_volatilidade_mercado';
COMMENT ON TABLE sandbox.indicador_atividade_mercado        IS '[sandbox] Espelho de indicators.indicador_atividade_mercado';
COMMENT ON TABLE sandbox.indicador_confianca_mercado_local  IS '[sandbox] Espelho de indicators.indicador_confianca_mercado_local';
COMMENT ON TABLE sandbox.indicador_sentimento_noticias      IS '[sandbox] Espelho de indicators.indicador_sentimento_noticias';
COMMENT ON TABLE sandbox.analytics_dashboard_diario         IS '[sandbox] Espelho de analytics.analytics_dashboard_diario';
COMMENT ON TABLE sandbox.analytics_serie_temporal_ismb      IS '[sandbox] Espelho de analytics.analytics_serie_temporal_ismb';
COMMENT ON TABLE sandbox.analytics_correlacao               IS '[sandbox] Espelho de analytics.analytics_correlacao';
COMMENT ON TABLE sandbox.analytics_kpis_agregados           IS '[sandbox] Espelho de analytics.analytics_kpis_agregados';
COMMENT ON TABLE sandbox.pipeline_logs                      IS '[sandbox] Espelho de governance.pipeline_logs';
COMMENT ON TABLE sandbox.validation_data_mercado            IS '[sandbox] Espelho de governance.validation_data_mercado';
COMMENT ON TABLE sandbox.validation_data_indicadores        IS '[sandbox] Espelho de governance.validation_data_indicadores';
COMMENT ON TABLE sandbox.validation_indice_isbm             IS '[sandbox] Espelho de governance.validation_indice_isbm';
COMMENT ON TABLE sandbox.metrics_stage_cds                  IS '[sandbox] Espelho de governance.metrics_stage_cds';
COMMENT ON TABLE sandbox.metrics_stage_ibov                 IS '[sandbox] Espelho de governance.metrics_stage_ibov';
COMMENT ON TABLE sandbox.metrics_stage_ivvb                 IS '[sandbox] Espelho de governance.metrics_stage_ivvb';
COMMENT ON TABLE sandbox.metrics_stage_ifix                 IS '[sandbox] Espelho de governance.metrics_stage_ifix';
COMMENT ON TABLE sandbox.metrics_stage_infomoney            IS '[sandbox] Espelho de governance.metrics_stage_infomoney';
COMMENT ON TABLE sandbox.metrics_stage_valorinveste         IS '[sandbox] Espelho de governance.metrics_stage_valorinveste';
COMMENT ON TABLE sandbox.metrics_stage_seudinheiro          IS '[sandbox] Espelho de governance.metrics_stage_seudinheiro';
COMMENT ON TABLE sandbox.metrics_stage_moneytimes           IS '[sandbox] Espelho de governance.metrics_stage_moneytimes';
COMMENT ON TABLE sandbox.data_quality_report                IS '[sandbox] Espelho de governance.data_quality_report';
