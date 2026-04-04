-- ISMB Database - dim_tempo
-- Ordem de execução: 08

-- Função: Cálculo da Páscoa
-- Algoritmo Meeus/Jones/Butcher - válido para 1900-2099, O(1), IMMUTABLE.
-- Referência: https://en.wikipedia.org/wiki/Date_of_Easter

CREATE OR REPLACE FUNCTION curated.fn_easter(p_year INT)
RETURNS DATE
LANGUAGE plpgsql
IMMUTABLE STRICT
AS $$
DECLARE
    a  INT := p_year % 19;
    b  INT := p_year / 100;
    c  INT := p_year % 100;
    d  INT := b / 4;
    e  INT := b % 4;
    f  INT := (b + 8) / 25;
    g  INT := (b - f + 1) / 3;
    h  INT := (19 * a + b - d - g + 15) % 30;
    i  INT := c / 4;
    k  INT := c % 4;
    l  INT := (32 + 2 * e + 2 * i - h - k) % 7;
    m  INT := (a + 11 * h + 22 * l) / 451;
    n  INT := h + l - 7 * m + 114;
BEGIN
    RETURN MAKE_DATE(p_year, n / 31, (n % 31) + 1);
END;
$$;

COMMENT ON FUNCTION curated.fn_easter(INT) IS
    'Calcula a data da Páscoa para o ano p_year usando o algoritmo Meeus/Jones/Butcher (válido 1900-2099).';


-- Função: Feriados nacionais brasileiros
-- Retorna todos os feriados (fixos + móveis) para um range de anos.

CREATE OR REPLACE FUNCTION curated.fn_feriados_br(p_start_y INT, p_end_y INT)
RETURNS TABLE(feriado_dt DATE, nome TEXT)
LANGUAGE sql
IMMUTABLE STRICT
AS $$
    -- Feriados nacionais FIXOS
    SELECT MAKE_DATE(y, 1, 1),   'Confraternização Universal' FROM generate_series(p_start_y, p_end_y) AS g(y)
    UNION ALL
    SELECT MAKE_DATE(y, 4, 21),  'Tiradentes'                 FROM generate_series(p_start_y, p_end_y) AS g(y)
    UNION ALL
    SELECT MAKE_DATE(y, 5, 1),   'Dia do Trabalho'            FROM generate_series(p_start_y, p_end_y) AS g(y)
    UNION ALL
    SELECT MAKE_DATE(y, 9, 7),   'Independência do Brasil'    FROM generate_series(p_start_y, p_end_y) AS g(y)
    UNION ALL
    SELECT MAKE_DATE(y, 10, 12), 'Nossa Senhora Aparecida'    FROM generate_series(p_start_y, p_end_y) AS g(y)
    UNION ALL
    SELECT MAKE_DATE(y, 11, 2),  'Finados'                    FROM generate_series(p_start_y, p_end_y) AS g(y)
    UNION ALL
    SELECT MAKE_DATE(y, 11, 15), 'Proclamação da República'   FROM generate_series(p_start_y, p_end_y) AS g(y)
    UNION ALL
    SELECT MAKE_DATE(y, 11, 20), 'Dia da Consciência Negra'   FROM generate_series(p_start_y, p_end_y) AS g(y)
    UNION ALL
    SELECT MAKE_DATE(y, 12, 25), 'Natal'                      FROM generate_series(p_start_y, p_end_y) AS g(y)
    UNION ALL
    SELECT (curated.fn_easter(y) - INTERVAL '48 days')::DATE, 'Carnaval Segunda-feira'
    FROM generate_series(p_start_y, p_end_y) AS g(y)
    UNION ALL
    SELECT (curated.fn_easter(y) - INTERVAL '47 days')::DATE, 'Carnaval Terça-feira'
    FROM generate_series(p_start_y, p_end_y) AS g(y)
    UNION ALL
    SELECT (curated.fn_easter(y) - INTERVAL '2 days')::DATE, 'Sexta-feira Santa'
    FROM generate_series(p_start_y, p_end_y) AS g(y)
    UNION ALL
    SELECT (curated.fn_easter(y) + INTERVAL '60 days')::DATE, 'Corpus Christi'
    FROM generate_series(p_start_y, p_end_y) AS g(y)
$$;

COMMENT ON FUNCTION curated.fn_feriados_br(INT, INT) IS
    'Retorna todos os feriados nacionais brasileiros (fixos + móveis via Páscoa) para o intervalo de anos [p_start_y, p_end_y].';


-- Procedure principal: populate dim_tempo

CREATE OR REPLACE PROCEDURE curated.proc_populate_dim_tempo(
    p_start DATE DEFAULT '2017-01-01',
    p_end   DATE DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_end        DATE;
    v_start_year INT;
    v_end_year   INT;
    v_rows       BIGINT;
BEGIN
    v_end        := COALESCE(p_end, MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::INT + 10, 1, 1));
    v_start_year := EXTRACT(YEAR FROM p_start)::INT;
    v_end_year   := EXTRACT(YEAR FROM v_end)::INT;

    DELETE FROM curated.dim_tempo
    WHERE dat_ref::DATE BETWEEN p_start AND v_end;

    INSERT INTO curated.dim_tempo (
        sk_tempo, dat_ref, ano, mes, dia_mes, nome_mes,
        nome_dia_semana, dia_semana, trimestre, semana_ano,
        dia_util, feriado, nome_feriado, fim_semana
    )
    WITH

    spine AS (
        SELECT d::DATE AS dat_ref_date
        FROM generate_series(p_start, v_end, INTERVAL '1 day') AS g(d)
    ),

    holidays AS (
        SELECT feriado_dt, nome
        FROM curated.fn_feriados_br(v_start_year, v_end_year)
    ),

    base AS (
        SELECT
            TO_CHAR(s.dat_ref_date, 'YYYY-MM-DD')                    AS dat_ref,
            TO_CHAR(s.dat_ref_date, 'YYYYMMDD')::INT                 AS sk_tempo,
            EXTRACT(YEAR    FROM s.dat_ref_date)::SMALLINT           AS ano,
            EXTRACT(MONTH   FROM s.dat_ref_date)::SMALLINT           AS mes,
            EXTRACT(DAY     FROM s.dat_ref_date)::SMALLINT           AS dia_mes,
            EXTRACT(QUARTER FROM s.dat_ref_date)::SMALLINT           AS trimestre,
            (EXTRACT(ISODOW FROM s.dat_ref_date)::INT - 1)::SMALLINT AS dia_semana,
            EXTRACT(WEEK FROM s.dat_ref_date)::SMALLINT              AS semana_ano,
            COALESCE(h.nome, '')                                      AS nome_feriado,
            (h.feriado_dt IS NOT NULL)                                AS is_holiday
        FROM spine s
        LEFT JOIN holidays h ON h.feriado_dt = s.dat_ref_date
    )

    SELECT
        b.sk_tempo,
        b.dat_ref,
        b.ano,
        b.mes,
        b.dia_mes,

        (ARRAY[
            'Janeiro','Fevereiro','Março','Abril','Maio','Junho',
            'Julho','Agosto','Setembro','Outubro','Novembro','Dezembro'
        ])[b.mes] AS nome_mes,

        (ARRAY[
            'Segunda','Terça','Quarta','Quinta','Sexta','Sábado','Domingo'
        ])[b.dia_semana + 1] AS nome_dia_semana,

        b.dia_semana,
        b.trimestre,
        b.semana_ano,

        CASE
            WHEN b.dia_semana < 5
             AND NOT b.is_holiday
             AND NOT (b.mes = 12 AND b.dia_mes IN (24, 31))
            THEN 1
            ELSE 0
        END AS dia_util,

        CASE WHEN b.is_holiday THEN 1 ELSE 0 END AS feriado,

        b.nome_feriado,

        CASE WHEN b.dia_semana >= 5 THEN 1 ELSE 0 END AS fim_semana

    FROM base b
    ORDER BY b.sk_tempo;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RAISE NOTICE '[proc_populate_dim_tempo] Range: % -> % | Inseridas: % linhas',
        p_start, v_end, v_rows;
END;
$$;

COMMENT ON PROCEDURE curated.proc_populate_dim_tempo(DATE, DATE) IS
    'Gera (ou re-gera) a dimensão de tempo para o intervalo [p_start, p_end]. '
    'Idempotente: apaga e re-insere o range. '
    'Padrão: 2017-01-01 até current_year+10. '
    'Espelha a lógica do pipeline build_schema/nodes.py::build_dim_tempo().';


-- Execução inicial

CALL curated.proc_populate_dim_tempo(
    p_start => '2017-01-01'::DATE,
    p_end   => MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::INT + 10, 1, 1)
);
