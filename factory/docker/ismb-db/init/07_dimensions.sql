-- ISMB Database
-- Popula as dimensões
-- Ordem de execução: 07

-- curated.dim_indice
INSERT INTO curated.dim_indice (sk_indice, cod_indice, nome_indice, categoria, descricao)
VALUES
    (1, 'CDS',  'Credit Default Swap Brasil 5 Anos',  'Risco Soberano',      'Medida de risco de crédito do Brasil'),
    (2, 'IBOV', 'Indice Bovespa',                     'Mercado Acionario',   'Principal indice de ações da B3'),
    (3, 'IVVB', 'IVVB - VIX Brasil',                  'Volatilidade',        'Indice de volatilidade do mercado brasileiro'),
    (4, 'IFIX', 'Indice de Fundos Imobiliarios',      'Mercado Imobiliario', 'Indice de fundos imobiliarios da B3')
ON CONFLICT (sk_indice) DO UPDATE
    SET cod_indice  = EXCLUDED.cod_indice,
        nome_indice = EXCLUDED.nome_indice,
        categoria   = EXCLUDED.categoria,
        descricao   = EXCLUDED.descricao;

-- curated.dim_fonte_noticia
INSERT INTO curated.dim_fonte_noticia (sk_fonte, cod_fonte, nome_fonte, url, confiabilidade)
VALUES
    (1, 'INFOMONEY',   'InfoMoney',    'https://www.infomoney.com.br',     'Alta'),
    (2, 'VALORINVEST', 'Valor Investe','https://valorinveste.globo.com/',  'Alta'),
    (3, 'SEUDINHEIRO', 'Seu Dinheiro', 'https://www.seudinheiro.com/',     'Media'),
    (4, 'MONEYTIMES',  'Money Times',  'https://www.moneytimes.com.br',    'Media')
ON CONFLICT (sk_fonte) DO UPDATE
    SET cod_fonte      = EXCLUDED.cod_fonte,
        nome_fonte     = EXCLUDED.nome_fonte,
        url            = EXCLUDED.url,
        confiabilidade = EXCLUDED.confiabilidade;

-- curated.dim_indicador
INSERT INTO curated.dim_indicador (sk_indicador, cod_indicador, nome_indicador, descricao, peso_ismb)
VALUES
    (1, 'RISCO_CREDITO',       'Risco de Credito',           'Indicador baseado em CDS e volatilidade',                             0.25),
    (2, 'RETORNO_MERCADO',     'Retorno do Mercado',         'Indicador baseado em retornos do Ibovespa',                           0.20),
    (3, 'VOLATILIDADE',        'Volatilidade do Mercado',    'Indicador baseado em EWMA, ATR e IVVB',                               0.20),
    (4, 'ATIVIDADE',           'Atividade do Mercado',       'Indicador baseado em volume e desvio de retorno',                     0.10),
    (5, 'CONFIANCA_LOCAL',     'Confianca do Mercado Local', 'Indicador baseado no IFIX',                                           0.10),
    (6, 'SENTIMENTO_NOTICIAS', 'Sentimento de Noticias',     'Indicador baseado em analise de sentimento de portais financeiros',   0.15)
ON CONFLICT (sk_indicador) DO UPDATE
    SET cod_indicador  = EXCLUDED.cod_indicador,
        nome_indicador = EXCLUDED.nome_indicador,
        descricao      = EXCLUDED.descricao,
        peso_ismb      = EXCLUDED.peso_ismb;

-- Verificação de integridade: soma dos pesos deve ser 1.00
DO $$
DECLARE
    v_soma NUMERIC;
BEGIN
    SELECT SUM(peso_ismb) INTO v_soma FROM curated.dim_indicador;
    IF v_soma <> 1.00 THEN
        RAISE EXCEPTION
            'Integridade violada: soma dos pesos dos indicadores = % (esperado 1.00)',
            v_soma;
    END IF;
    RAISE NOTICE 'dim_indicador: soma dos pesos = % ✓', v_soma;
END;
$$;
