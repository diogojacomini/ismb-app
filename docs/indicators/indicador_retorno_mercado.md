# Indicador de Retorno do Mercado (Ibovespa)

Este indicador resume, como o desempenho recente do Ibovespa se compara ao seu padrão histórico e à força do volume de negociações. Em termos simples, ele transforma o “quão bom ou ruim” foi o retorno recente.

## O que ele nos mostra
- Próximo de 0: desempenho fraco em relação ao histórico recente.
- Faixa intermediária: retorno dentro do padrão.
- Próximo de 100: desempenho acima do esperado para o período.

## Como é calculado
Com base nas séries diárias de fechamento (close) e volume:

1) Retorno do dia (logarítmico):  log_ret_t = ln(close_t / close_{t-1})
2) Comportamento típico (janelas móveis):
	- media_ret_t = média móvel de log_ret (padrão: 21 dias)
	- desvio_ret_t = desvio-padrão móvel de log_ret (padrão: 21 dias)
3) Desempenho relativo do dia:  z_retorno_t = (log_ret_t − media_ret_t) / desvio_ret_t
4) Score base (0–100): normalizamos z_retorno_t para 0–100 usando o histórico recente
5) Relevância do volume:
	- media_vol = média móvel do volume (padrão: 30 dias)
	- fator_vol = max(volume_t / media_vol, piso)  (padrão do piso: 0,7)
6) Score final (0–100): aplicamos o fator de volume ao score base e normalizamos novamente

Em resumo: resultados altos indicam desempenho acima do normal e bem suportado por volume; resultados baixos indicam desempenho fraco.

## Insumos e parâmetros
- Dados: close (fechamento) e volume diários do Ibovespa.
- Parâmetros típicos:
  - Janela para média/desvio do retorno: 21 dias.
  - Janela para média de volume: 30 dias.
  - Piso do fator de volume: 0,7.
