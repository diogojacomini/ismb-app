# Indicador de Atividade do Mercado

Este indicador mede a “intensidade” diária do mercado: se o dia foi mais fraco, normal ou forte em relação ao ritmo recente. Onde um retorno alto sem volume tem menos qualidade.

## O que ele nos mostra
- Próximo de 0: dia fraco, movimento abaixo do ritmo recente.
- Faixa intermediária: atividade em linha com os últimos dias.
- Próximo de 100: dia forte, acima do normal e sustentado por volume.


## Como é calculado
Considere as séries diárias de preço de fechamento (close) e volume:

1) Retorno do dia (em %):  retorno_t = ((close_t / close_{t-1}) − 1) × 100
2) Ritmo recente:  retorno_mm_t = média dos últimos N dias (padrão: N = 3)
3) Desvio relativo do dia:  desvio_t = (retorno_t / retorno_mm_t) − 1
4) Ajuste por volume:  fator_vol_t = max(volume_t / média_{30d}(volume), limite_mínimo)  (padrão: limite_mínimo = 0,7)
5) Score bruto (0–100): normalizamos desvio_t para 0–100 usando o histórico recente (faixa baseada em percentis)
6) Score final (0–100): aplicamos o ajuste de volume e normalizamos novamente

Em resumo: dia forte = desvio positivo acima da média recente, com bom volume. Dia fraco = desvio abaixo da média e/ou volume fraco.

## Insumos e parâmetros
- Dados: preço de fechamento diário e volume do IBOVESPA.
- Parâmetros típicos:
	- Janela do retorno médio (N): 3 dias.
	- Janela da média de volume: 30 dias.
	- Piso do fator de volume: 0,7 (evita penalização excessiva).
