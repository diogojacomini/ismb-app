# Indicador de Confiança do Mercado Local (IFIX)

Este indicador acompanha a confiança do mercado brasileiro por meio do IFIX, mostrando se o dia foi melhor, igual ou pior que o ritmo recente. O resultado vai de 0 a 100 e facilita a leitura da "temperatura" local.

## O que ele nos mostra
- Próximo de 0: confiança baixa, retornos aquém da média recente.
- Faixa intermediária: confiança estável, retornos dentro do padrão.
- Próximo de 100: confiança elevada, retornos acima do normal recente.

## Como é calculado
Usa o de preço de fechamento do IFIX:

1) Retorno do dia (em %):  retorno_t = ((close_t / close_{t-1}) − 1) × 100
2) Ritmo recente:  retorno_mm_t = média dos últimos N dias (padrão: N = 3)
3) Desvio relativo do dia:  desvio_t = (retorno_t / retorno_mm_t) − 1
4) Score (0–100): normalizamos desvio_t para 0–100 com base no histórico (faixa por percentis)

Em resumo, o indicador sobe quando o retorno do dia supera a média recente e cai quando é inferior.

## Insumos e parâmetros
- Dado: close_price (fechamento diário do IFIX).
- Parâmetros típicos:
	- Janela do retorno médio (N): 3 dias.
	- Lookback de comparação histórica: ~30 dias (ajustável ao seu histórico).
