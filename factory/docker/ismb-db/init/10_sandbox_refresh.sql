-- ISMB Database - Sandbox: Procedure de refresh (prod -> sandbox)


CREATE OR REPLACE PROCEDURE sandbox.proc_refresh_from_prod(
    p_dat_start          DATE    DEFAULT NULL,
    p_dat_end            DATE    DEFAULT NULL,
    p_include_governance BOOLEAN DEFAULT FALSE
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_start  DATE    := COALESCE(p_dat_start, '2000-01-01'::DATE);
    v_end    DATE    := COALESCE(p_dat_end,   CURRENT_DATE);
    v_rows   BIGINT  := 0;
    v_total  BIGINT  := 0;

    v_report TEXT    := '';
BEGIN
    RAISE NOTICE '[proc_refresh_from_prod] Iniciando refresh: % -> % | governance=%',
        v_start, v_end, p_include_governance;

    TRUNCATE
        sandbox.dim_indice,
        sandbox.dim_tempo,
        sandbox.dim_indicador,
        sandbox.dim_fonte_noticia,
        sandbox.fato_transacao_mercado,
        sandbox.fato_transacao_noticias,
        sandbox.fato_indice_ismb,
        sandbox.indicador_risco_credito,
        sandbox.indicador_retorno_mercado,
        sandbox.indicador_volatilidade_mercado,
        sandbox.indicador_atividade_mercado,
        sandbox.indicador_confianca_mercado_local,
        sandbox.indicador_sentimento_noticias,
        sandbox.analytics_dashboard_diario,
        sandbox.analytics_serie_temporal_ismb,
        sandbox.analytics_correlacao,
        sandbox.analytics_kpis_agregados,
        sandbox.stage_cds,
        sandbox.stage_ibov,
        sandbox.stage_ivvb,
        sandbox.stage_ifix,
        sandbox.stage_infomoney,
        sandbox.stage_valorinveste,
        sandbox.stage_seudinheiro,
        sandbox.stage_moneytimes;

    IF p_include_governance THEN
        TRUNCATE
            sandbox.pipeline_logs,
            sandbox.validation_data_mercado,
            sandbox.validation_data_indicadores,
            sandbox.validation_indice_isbm,
            sandbox.metrics_stage_cds,
            sandbox.metrics_stage_ibov,
            sandbox.metrics_stage_ivvb,
            sandbox.metrics_stage_ifix,
            sandbox.metrics_stage_infomoney,
            sandbox.metrics_stage_valorinveste,
            sandbox.metrics_stage_seudinheiro,
            sandbox.metrics_stage_moneytimes,
            sandbox.data_quality_report;
    END IF;


    -- Dimensões estáticas - copiadas por inteiro (sem filtro de data)

    INSERT INTO sandbox.dim_indice
    SELECT * FROM curated.dim_indice;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    v_report := v_report || format(E'\n  dim_indice              : %s', v_rows);
    v_total  := v_total + v_rows;

    INSERT INTO sandbox.dim_indicador
    SELECT * FROM curated.dim_indicador;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    v_report := v_report || format(E'\n  dim_indicador           : %s', v_rows);
    v_total  := v_total + v_rows;

    INSERT INTO sandbox.dim_fonte_noticia
    SELECT * FROM curated.dim_fonte_noticia;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    v_report := v_report || format(E'\n  dim_fonte_noticia       : %s', v_rows);
    v_total  := v_total + v_rows;


    -- dim_tempo - filtrada por data

    INSERT INTO sandbox.dim_tempo
    SELECT * FROM curated.dim_tempo
    WHERE dat_ref::DATE BETWEEN v_start AND v_end;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    v_report := v_report || format(E'\n  dim_tempo               : %s', v_rows);
    v_total  := v_total + v_rows;


    -- Stage - séries de mercado

    INSERT INTO sandbox.stage_cds
    SELECT * FROM stage.stage_cds
    WHERE dat_ref::DATE BETWEEN v_start AND v_end;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    v_report := v_report || format(E'\n  stage_cds               : %s', v_rows);
    v_total  := v_total + v_rows;

    INSERT INTO sandbox.stage_ibov
    SELECT * FROM stage.stage_ibov
    WHERE dat_ref::DATE BETWEEN v_start AND v_end;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    v_report := v_report || format(E'\n  stage_ibov              : %s', v_rows);
    v_total  := v_total + v_rows;

    INSERT INTO sandbox.stage_ivvb
    SELECT * FROM stage.stage_ivvb
    WHERE dat_ref::DATE BETWEEN v_start AND v_end;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    v_report := v_report || format(E'\n  stage_ivvb              : %s', v_rows);
    v_total  := v_total + v_rows;

    INSERT INTO sandbox.stage_ifix
    SELECT * FROM stage.stage_ifix
    WHERE dat_ref::DATE BETWEEN v_start AND v_end;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    v_report := v_report || format(E'\n  stage_ifix              : %s', v_rows);
    v_total  := v_total + v_rows;


    -- Stage - notícias
    INSERT INTO sandbox.stage_infomoney
    SELECT * FROM stage.stage_infomoney
    WHERE dat_ref::DATE BETWEEN v_start AND v_end;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    v_report := v_report || format(E'\n  stage_infomoney         : %s', v_rows);
    v_total  := v_total + v_rows;

    INSERT INTO sandbox.stage_valorinveste
    SELECT * FROM stage.stage_valorinveste
    WHERE dat_ref::DATE BETWEEN v_start AND v_end;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    v_report := v_report || format(E'\n  stage_valorinveste      : %s', v_rows);
    v_total  := v_total + v_rows;

    INSERT INTO sandbox.stage_seudinheiro
    SELECT * FROM stage.stage_seudinheiro
    WHERE dat_ref::DATE BETWEEN v_start AND v_end;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    v_report := v_report || format(E'\n  stage_seudinheiro       : %s', v_rows);
    v_total  := v_total + v_rows;

    INSERT INTO sandbox.stage_moneytimes
    SELECT * FROM stage.stage_moneytimes
    WHERE dat_ref::DATE BETWEEN v_start AND v_end;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    v_report := v_report || format(E'\n  stage_moneytimes        : %s', v_rows);
    v_total  := v_total + v_rows;


    -- Curated - fatos

    INSERT INTO sandbox.fato_transacao_mercado
    SELECT * FROM curated.fato_transacao_mercado
    WHERE dat_ref::DATE BETWEEN v_start AND v_end;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    v_report := v_report || format(E'\n  fato_transacao_mercado  : %s', v_rows);
    v_total  := v_total + v_rows;

    INSERT INTO sandbox.fato_transacao_noticias (dat_ref, cod_fonte, txt_titulo)
    SELECT dat_ref, cod_fonte, txt_titulo
    FROM curated.fato_transacao_noticias
    WHERE dat_ref::DATE BETWEEN v_start AND v_end;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    v_report := v_report || format(E'\n  fato_transacao_noticias : %s', v_rows);
    v_total  := v_total + v_rows;

    INSERT INTO sandbox.fato_indice_ismb
    SELECT * FROM curated.fato_indice_ismb
    WHERE dat_ref::DATE BETWEEN v_start AND v_end;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    v_report := v_report || format(E'\n  fato_indice_ismb        : %s', v_rows);
    v_total  := v_total + v_rows;


    -- Indicators

    INSERT INTO sandbox.indicador_risco_credito
    SELECT * FROM indicators.indicador_risco_credito
    WHERE dat_ref::DATE BETWEEN v_start AND v_end;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    v_report := v_report || format(E'\n  ind_risco_credito       : %s', v_rows);
    v_total  := v_total + v_rows;

    INSERT INTO sandbox.indicador_retorno_mercado
    SELECT * FROM indicators.indicador_retorno_mercado
    WHERE dat_ref::DATE BETWEEN v_start AND v_end;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    v_report := v_report || format(E'\n  ind_retorno_mercado     : %s', v_rows);
    v_total  := v_total + v_rows;

    INSERT INTO sandbox.indicador_volatilidade_mercado
    SELECT * FROM indicators.indicador_volatilidade_mercado
    WHERE dat_ref::DATE BETWEEN v_start AND v_end;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    v_report := v_report || format(E'\n  ind_volatilidade        : %s', v_rows);
    v_total  := v_total + v_rows;

    INSERT INTO sandbox.indicador_atividade_mercado
    SELECT * FROM indicators.indicador_atividade_mercado
    WHERE dat_ref::DATE BETWEEN v_start AND v_end;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    v_report := v_report || format(E'\n  ind_atividade           : %s', v_rows);
    v_total  := v_total + v_rows;

    INSERT INTO sandbox.indicador_confianca_mercado_local
    SELECT * FROM indicators.indicador_confianca_mercado_local
    WHERE dat_ref::DATE BETWEEN v_start AND v_end;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    v_report := v_report || format(E'\n  ind_confianca_local     : %s', v_rows);
    v_total  := v_total + v_rows;

    INSERT INTO sandbox.indicador_sentimento_noticias
    SELECT * FROM indicators.indicador_sentimento_noticias
    WHERE dat_ref::DATE BETWEEN v_start AND v_end;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    v_report := v_report || format(E'\n  ind_sentimento_noticias : %s', v_rows);
    v_total  := v_total + v_rows;


    -- Analytics

    INSERT INTO sandbox.analytics_dashboard_diario
    SELECT * FROM analytics.analytics_dashboard_diario
    WHERE dat_ref::DATE BETWEEN v_start AND v_end;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    v_report := v_report || format(E'\n  analytics_dashboard     : %s', v_rows);
    v_total  := v_total + v_rows;

    INSERT INTO sandbox.analytics_serie_temporal_ismb
    SELECT * FROM analytics.analytics_serie_temporal_ismb
    WHERE dat_ref::DATE BETWEEN v_start AND v_end;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    v_report := v_report || format(E'\n  analytics_serie_ismb    : %s', v_rows);
    v_total  := v_total + v_rows;

    INSERT INTO sandbox.analytics_correlacao
    SELECT * FROM analytics.analytics_correlacao
    WHERE dat_ref::DATE BETWEEN v_start AND v_end;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    v_report := v_report || format(E'\n  analytics_correlacao    : %s', v_rows);
    v_total  := v_total + v_rows;

    INSERT INTO sandbox.analytics_kpis_agregados
    SELECT * FROM analytics.analytics_kpis_agregados
    WHERE dat_ref::DATE BETWEEN v_start AND v_end;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    v_report := v_report || format(E'\n  analytics_kpis          : %s', v_rows);
    v_total  := v_total + v_rows;


    -- Governance

    IF p_include_governance THEN

        INSERT INTO sandbox.pipeline_logs
        SELECT * FROM governance.pipeline_logs
        WHERE dat_ref::DATE BETWEEN v_start AND v_end;
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_report := v_report || format(E'\n  pipeline_logs           : %s', v_rows);
        v_total  := v_total + v_rows;

        INSERT INTO sandbox.validation_data_mercado
        SELECT * FROM governance.validation_data_mercado
        WHERE dat_ref::DATE BETWEEN v_start AND v_end;
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_report := v_report || format(E'\n  valid_mercado           : %s', v_rows);
        v_total  := v_total + v_rows;

        INSERT INTO sandbox.validation_data_indicadores
        SELECT * FROM governance.validation_data_indicadores
        WHERE dat_ref::DATE BETWEEN v_start AND v_end;
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_report := v_report || format(E'\n  valid_indicadores       : %s', v_rows);
        v_total  := v_total + v_rows;

        INSERT INTO sandbox.validation_indice_isbm
        SELECT * FROM governance.validation_indice_isbm
        WHERE dat_ref::DATE BETWEEN v_start AND v_end;
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_report := v_report || format(E'\n  valid_indice_isbm       : %s', v_rows);
        v_total  := v_total + v_rows;

        INSERT INTO sandbox.metrics_stage_cds
        SELECT * FROM governance.metrics_stage_cds
        WHERE dat_ref::DATE BETWEEN v_start AND v_end;
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_report := v_report || format(E'\n  metrics_stage_cds       : %s', v_rows);
        v_total  := v_total + v_rows;

        INSERT INTO sandbox.metrics_stage_ibov
        SELECT * FROM governance.metrics_stage_ibov
        WHERE dat_ref::DATE BETWEEN v_start AND v_end;
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_report := v_report || format(E'\n  metrics_stage_ibov      : %s', v_rows);
        v_total  := v_total + v_rows;

        INSERT INTO sandbox.metrics_stage_ivvb
        SELECT * FROM governance.metrics_stage_ivvb
        WHERE dat_ref::DATE BETWEEN v_start AND v_end;
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_report := v_report || format(E'\n  metrics_stage_ivvb      : %s', v_rows);
        v_total  := v_total + v_rows;

        INSERT INTO sandbox.metrics_stage_ifix
        SELECT * FROM governance.metrics_stage_ifix
        WHERE dat_ref::DATE BETWEEN v_start AND v_end;
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_report := v_report || format(E'\n  metrics_stage_ifix      : %s', v_rows);
        v_total  := v_total + v_rows;

        INSERT INTO sandbox.metrics_stage_infomoney
        SELECT * FROM governance.metrics_stage_infomoney
        WHERE dat_ref::DATE BETWEEN v_start AND v_end;
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_report := v_report || format(E'\n  metrics_stage_infomoney : %s', v_rows);
        v_total  := v_total + v_rows;

        INSERT INTO sandbox.metrics_stage_valorinveste
        SELECT * FROM governance.metrics_stage_valorinveste
        WHERE dat_ref::DATE BETWEEN v_start AND v_end;
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_report := v_report || format(E'\n  metrics_stage_valor     : %s', v_rows);
        v_total  := v_total + v_rows;

        INSERT INTO sandbox.metrics_stage_seudinheiro
        SELECT * FROM governance.metrics_stage_seudinheiro
        WHERE dat_ref::DATE BETWEEN v_start AND v_end;
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_report := v_report || format(E'\n  metrics_stage_seud      : %s', v_rows);
        v_total  := v_total + v_rows;

        INSERT INTO sandbox.metrics_stage_moneytimes
        SELECT * FROM governance.metrics_stage_moneytimes
        WHERE dat_ref::DATE BETWEEN v_start AND v_end;
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_report := v_report || format(E'\n  metrics_stage_money     : %s', v_rows);
        v_total  := v_total + v_rows;

        INSERT INTO sandbox.data_quality_report
        SELECT * FROM governance.data_quality_report
        WHERE dat_ref::DATE BETWEEN v_start AND v_end;
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_report := v_report || format(E'\n  data_quality_report     : %s', v_rows);
        v_total  := v_total + v_rows;

    END IF;


    -- Relatório final
    RAISE NOTICE E'[proc_refresh_from_prod] Concluído.\nTabela                      | Linhas%s\n─────────────────────────────────────────\nTOTAL                       : %s',
        v_report, v_total;

END;
$$;

COMMENT ON PROCEDURE sandbox.proc_refresh_from_prod(DATE, DATE, BOOLEAN) IS
    'Copia dados de produção para o sandbox no intervalo [p_dat_start, p_dat_end]. '
    'Idempotente: TRUNCATE + INSERT. '
    'p_include_governance=TRUE copia também tabelas de auditoria/qualidade. '
    'Exemplo: CALL sandbox.proc_refresh_from_prod(''2025-01-01'', ''2025-12-31''); '
    'Exemplo completo: CALL sandbox.proc_refresh_from_prod(NULL, NULL, TRUE);';


-- Conveniência: view que lista o estado atual do sandbox vs produção

CREATE OR REPLACE VIEW sandbox.vw_sandbox_status AS
WITH stats AS (
    SELECT 'stage_cds'                  AS tabela, COUNT(*) AS n_sandbox, MIN(dat_ref) AS dat_min, MAX(dat_ref) AS dat_max FROM sandbox.stage_cds
    UNION ALL SELECT 'stage_ibov',          COUNT(*), MIN(dat_ref), MAX(dat_ref) FROM sandbox.stage_ibov
    UNION ALL SELECT 'stage_ivvb',          COUNT(*), MIN(dat_ref), MAX(dat_ref) FROM sandbox.stage_ivvb
    UNION ALL SELECT 'stage_ifix',          COUNT(*), MIN(dat_ref), MAX(dat_ref) FROM sandbox.stage_ifix
    UNION ALL SELECT 'stage_infomoney',     COUNT(*), MIN(dat_ref), MAX(dat_ref) FROM sandbox.stage_infomoney
    UNION ALL SELECT 'stage_valorinveste',  COUNT(*), MIN(dat_ref), MAX(dat_ref) FROM sandbox.stage_valorinveste
    UNION ALL SELECT 'stage_seudinheiro',   COUNT(*), MIN(dat_ref), MAX(dat_ref) FROM sandbox.stage_seudinheiro
    UNION ALL SELECT 'stage_moneytimes',    COUNT(*), MIN(dat_ref), MAX(dat_ref) FROM sandbox.stage_moneytimes
    UNION ALL SELECT 'dim_tempo',           COUNT(*), MIN(dat_ref), MAX(dat_ref) FROM sandbox.dim_tempo
    UNION ALL SELECT 'fato_trans_mercado',  COUNT(*), MIN(dat_ref), MAX(dat_ref) FROM sandbox.fato_transacao_mercado
    UNION ALL SELECT 'fato_trans_noticias', COUNT(*), MIN(dat_ref), MAX(dat_ref) FROM sandbox.fato_transacao_noticias
    UNION ALL SELECT 'fato_indice_ismb',    COUNT(*), MIN(dat_ref), MAX(dat_ref) FROM sandbox.fato_indice_ismb
    UNION ALL SELECT 'ind_risco_credito',   COUNT(*), MIN(dat_ref), MAX(dat_ref) FROM sandbox.indicador_risco_credito
    UNION ALL SELECT 'ind_retorno_mercado', COUNT(*), MIN(dat_ref), MAX(dat_ref) FROM sandbox.indicador_retorno_mercado
    UNION ALL SELECT 'ind_volatilidade',    COUNT(*), MIN(dat_ref), MAX(dat_ref) FROM sandbox.indicador_volatilidade_mercado
    UNION ALL SELECT 'ind_atividade',       COUNT(*), MIN(dat_ref), MAX(dat_ref) FROM sandbox.indicador_atividade_mercado
    UNION ALL SELECT 'ind_confianca_local', COUNT(*), MIN(dat_ref), MAX(dat_ref) FROM sandbox.indicador_confianca_mercado_local
    UNION ALL SELECT 'ind_sentimento',      COUNT(*), MIN(dat_ref), MAX(dat_ref) FROM sandbox.indicador_sentimento_noticias
    UNION ALL SELECT 'analytics_dashboard', COUNT(*), MIN(dat_ref), MAX(dat_ref) FROM sandbox.analytics_dashboard_diario
    UNION ALL SELECT 'analytics_serie',     COUNT(*), MIN(dat_ref), MAX(dat_ref) FROM sandbox.analytics_serie_temporal_ismb
    UNION ALL SELECT 'analytics_corr',      COUNT(*), MIN(dat_ref), MAX(dat_ref) FROM sandbox.analytics_correlacao
    UNION ALL SELECT 'analytics_kpis',      COUNT(*), MIN(dat_ref), MAX(dat_ref) FROM sandbox.analytics_kpis_agregados
)
SELECT
    tabela,
    n_sandbox,
    dat_min,
    dat_max,
    (dat_max::DATE - dat_min::DATE) AS dias_cobertura,
    CURRENT_DATE - dat_max::DATE AS dias_defasagem
FROM stats
ORDER BY tabela;

COMMENT ON VIEW sandbox.vw_sandbox_status IS
    'Mostra o estado atual de cada tabela no sandbox: contagem, range de datas e defasagem em relação a hoje.';
