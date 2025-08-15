# Indicador de Sentimento da Mídia

Este indicador capta o “humor” de notícias econômicas no dia. Ele lê o tom dos títulos (positivo, negativo ou neutro) e resume em o clima informacional.

## O que ele nos mostra
- Próximo de 0: predominância de notícias com tom mais negativo.
- Faixa intermediária: equilíbrio entre notícias positivas e negativas.
- Próximo de 100: predominância de tom positivo nas manchetes.

## Como é calculado
Baseado em análise de sentimento dos títulos (ex.: VADER em português adaptado):

1) Para cada manchete, calculamos os escores: negativo, neutro, positivo e um composto (−1 a 1).
2) Agregamos no dia: média do composto das manchetes válidas.
3) Normalizamos para 0–100: score = 50 · (composto_médio + 1).
4) Opcional: aplicar suavização (média móvel curta de 3–5 dias) para reduzir ruído diário.

## Insumos e parâmetros
- Dados: títulos/manchetes do dia (fontes financeiras acompanhadas pelo pipeline).
- Parâmetros típicos:
	- Mínimo de manchetes/dia para cálculo: 5 (abaixo disso, trate como confiança baixa ou carregue leitura anterior).
	- Suavização opcional: média móvel 3–5 dias.
	- Dicionários/polaridade: revisar termos específicos do mercado brasileiro para melhor acurácia.
