# Indicador de Volatilidade do Mercado

Este indicador mede o “nível de nervosismo” do mercado. Ele sintetiza, em um único número, o quanto os preços têm oscilado recentemente no Brasil (Ibovespa) e o quanto essas oscilações estão alinhadas ao humor externo (via IVVB11). Números mais baixos indicam maior turbulência e números mais altos indicam um ambiente mais estável.

## O que ele nos mostra
- Próximo de 0: mercado tenso, oscilações elevadas, maior aversão a risco.
- Faixa intermediária: mercado em equilíbrio, oscilações dentro do padrão histórico.
- Próximo de 100: mercado calmo, oscilações contidas, maior previsibilidade.

## Como é construído (visão simples)
O indicador combina três perspectivas complementares de oscilação de preços:
1) Oscilação recente do Ibovespa ao longo dos dias (com maior peso em movimentos mais atuais).
2) Amplitude diária do Ibovespa (diferença entre máximas e mínimas do dia), que captura “ruído intradiário”.
3) Sinal externo via IVVB11, refletindo o humor internacional percebido localmente.

Cada uma dessas dimensões é convertida para uma mesma escala comparável. Em seguida, fazemos uma média ponderada e, por fim, traduzimos o resultado para a escala 0–100, onde valores menores representam maior volatilidade percebida.

## Como é calculado
Com base em séries diárias de preços do Ibovespa (close, high, low) e do IVVB11 (close):

1) Volatilidade recente (EWMA) do Ibovespa:
	- calcule os retornos logarítmicos diários do Ibovespa
	- aplique EWMA (janela 21; α típico 0,94) ⇒ vol_ewma_bova_t
2) Amplitude diária do Ibovespa (True Range simplificado):
	- tr_t = high_t − low_t
	- aplique média móvel/EMA (padrão: 14 dias) ⇒ atr_t
3) Sinal externo (IVVB11):
	- calcule vol_ewma do IVVB11 como no passo 1 ⇒ vol_ewma_ivvb_t
4) Normalização e combinação:
	- z_bova = normalizar(vol_ewma_bova_t)
	- z_atr = normalizar(atr_t)
	- z_ivvb = normalizar(vol_ewma_ivvb_t)
	- z_mix = w1·z_bova + w2·z_atr + w3·z_ivvb  (ex.: w1=0,5; w2=0,3; w3=0,2)
5) Inversão para leitura intuitiva e escala 0–100:
	- score_base = normalizar(−z_mix)
	- score_final = reescalar score_base para 0–100 (maior ⇒ mais estável)

## Insumos e parâmetros
- Dados: séries diárias do Ibovespa (close, high, low) e IVVB11 (close).
- Parâmetros típicos:
  - EWMA: janela 21, α=0,94.
  - ATR: janela 14 (EMA ou média simples).
  - Pesos (exemplo): w1=0,5; w2=0,3; w3=0,2.
