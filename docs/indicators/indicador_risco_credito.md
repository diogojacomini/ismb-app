# Indicador de Risco de Crédito

Este indicador traduz o equilíbrio entre medo e confiança do mercado. Ao unir o comportamento recente da oscilação dos preços com o desempenho observado, ele sintetiza o “apetite por risco” do dia.

## O que ele nos mostra
- Próximo de 0: predomínio de cautela e aversão ao risco.
- Faixa intermediária: mercado em modo neutro, sem sinais fortes de estresse ou euforia.
- Próximo de 100: ambiente de maior confiança e tomada de risco.

## Por que isso é útil
- Ajuda a calibrar exposição a ativos de risco ao longo do tempo.
- Oferece um termômetro simples para comunicar condições de mercado.
- Complementa a leitura de preço com um sinal de “sentimento” agregado.

## Como é calculado (fórmula simples)
Com base nas séries diárias do Ibovespa (close e, opcionalmente, high/low):

1) Retorno do dia (logarítmico):  log_ret_t = ln(close_t / close_{t-1})
2) Comportamento típico (janelas móveis):
	- média_ret_t = média móvel de log_ret (padrão: 21 dias)
	- desvio_ret_t = desvio-padrão móvel de log_ret (padrão: 21 dias)
3) Sinal de retorno:  z_retorno_t = (log_ret_t − média_ret_t) / desvio_ret_t
4) Sinal de risco (volatilidade):
	- vol_ewma_t = volatilidade EWMA dos retornos (padrão: 21 dias; α típico 0,94)
	- z_vol_t = normalização de vol_ewma_t (maior vol ⇒ menor apetite a risco)
5) Combinação dos sinais (0–100):
	- score_base = w_ret · normalizar(z_retorno_t) + w_vol · normalizar(−z_vol_t)
	- score_final = reescalar score_base para 0–100

Em resumo: retornos acima do típico e menor volatilidade elevam o score (maior apetite a risco); o oposto reduz o score.

## Insumos e parâmetros
- Dados: preços de fechamento diários do Ibovespa (close) e, opcionalmente, máximas/mínimas (high/low) se for usar amplitude.
- Parâmetros típicos:
  - Janela para média/desvio dos retornos: 21 dias.
  - EWMA da volatilidade: janela 21, α=0,94 (ajuste conforme seu histórico).
  - Pesos: w_ret=0,6 e w_vol=0,4 (ajuste conforme sua preferência de sensibilidade).
