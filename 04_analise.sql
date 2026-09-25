-- Databricks notebook source
-- MAGIC %md
-- MAGIC  04 - Análise: respostas às perguntas de negócio
-- MAGIC
-- MAGIC  Todas as consultas leem apenas a camada Gold. Cada resultado consta como uma screenshot no README.
-- MAGIC  

-- COMMAND ----------

USE CATALOG mvp_economia_guerra;
USE SCHEMA gold;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Pergunta 1** — Variação do PIB por território e período

-- COMMAND ----------

SELECT t.nome AS territorio, p.ano, p.trimestre, fo.nome_fonte AS 
    fonte, f.variacao_pib_pct
FROM fato_atividade_economica f
JOIN dim_territorio t ON f.id_territorio = t.id_territorio
JOIN dim_periodo p ON f.id_periodo = p.id_periodo
JOIN dim_fonte fo ON f.id_fonte = fo.id_fonte
WHERE f.variacao_pib_pct IS NOT NULL AND f.id_setor IS NULL
  AND p.ano >= 2022
ORDER BY territorio, p.ano, p.trimestre NULLS FIRST;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Pergunta 2** — Setores mais e menos afetados (Q1-2024 vs Q1-2023)
-- MAGIC
-- MAGIC Ranking por território e diferença Gaza − Cisjordânia por setor.

-- COMMAND ----------

SELECT t.nome AS territorio, s.nome_setor AS setor, f.variacao_pib_pct,
       RANK() OVER (PARTITION BY t.nome ORDER BY f.variacao_pib_pct ASC) AS ranking_mais_afetado
FROM fato_atividade_economica f
JOIN dim_territorio t ON f.id_territorio = t.id_territorio
JOIN dim_setor s ON f.id_setor = s.id_setor
ORDER BY territorio, ranking_mais_afetado;
 

-- COMMAND ----------

SELECT s.nome_setor AS setor,
       MAX(CASE WHEN t.nome = 'Cisjordania' THEN f.variacao_pib_pct END) AS cisjordania_pct,
       MAX(CASE WHEN t.nome = 'Gaza' THEN f.variacao_pib_pct END) AS gaza_pct,
       MAX(CASE WHEN t.nome = 'Gaza' THEN f.variacao_pib_pct END)
     - MAX(CASE WHEN t.nome = 'Cisjordania' THEN f.variacao_pib_pct END) AS diferenca_pp
FROM fato_atividade_economica f
JOIN dim_territorio t ON f.id_territorio = t.id_territorio
JOIN dim_setor s ON f.id_setor = s.id_setor
GROUP BY s.nome_setor
ORDER BY diferenca_pp;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC **Pergunta 3** — Evolução da taxa de desemprego

-- COMMAND ----------

SELECT t.nome AS territorio,
       MAX(CASE WHEN p.ano = 2022 THEN f.taxa_desemprego_pct END) AS desemprego_2022,
       MAX(CASE WHEN p.ano = 2023 THEN f.taxa_desemprego_pct END) AS desemprego_2023,
       MAX(CASE WHEN p.ano = 2024 THEN f.taxa_desemprego_pct END) AS desemprego_2024,
       MAX(CASE WHEN p.ano = 2024 THEN f.taxa_desemprego_pct END)
     - MAX(CASE WHEN p.ano = 2023 THEN f.taxa_desemprego_pct END) AS variacao_2023_2024_pp
FROM fato_atividade_economica f
JOIN dim_territorio t ON f.id_territorio = t.id_territorio
JOIN dim_periodo p ON f.id_periodo = p.id_periodo
WHERE f.id_setor IS NULL AND p.trimestre IS NULL AND p.mes IS NULL
  AND t.nome IN ('Israel', 'Cisjordania', 'Gaza')
GROUP BY t.nome
ORDER BY t.nome;
 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Pergunta 4**  — Negócios afetados em Israel (pesquisa emergencial CBS)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC - Evolução out → nov/2023 por distrito

-- COMMAND ----------

SELECT n.recorte_nome AS distrito,
       MAX(CASE WHEN p.mes = 10 THEN n.pct_emprego_minimo END) AS emprego_minimo_out,
       MAX(CASE WHEN p.mes = 11 THEN n.pct_emprego_minimo END) AS emprego_minimo_nov,
       MAX(CASE WHEN p.mes = 10 THEN n.pct_queda_receita_severa END) AS queda_receita_out,
       MAX(CASE WHEN p.mes = 11 THEN n.pct_queda_receita_severa END) AS queda_receita_nov
FROM fato_pesquisa_negocios n
JOIN dim_periodo p ON n.id_periodo = p.id_periodo
WHERE n.recorte_tipo = 'distrito'
GROUP BY n.recorte_nome
ORDER BY emprego_minimo_out DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC - Evolução por setor (queda de receita > 50%)

-- COMMAND ----------

SELECT n.recorte_nome AS setor,
       MAX(CASE WHEN p.mes = 10 THEN n.pct_queda_receita_severa END) AS queda_receita_out,
       MAX(CASE WHEN p.mes = 11 THEN n.pct_queda_receita_severa END) AS queda_receita_nov,
       MAX(CASE WHEN p.mes = 11 THEN n.pct_emprego_minimo END) AS emprego_minimo_nov
FROM fato_pesquisa_negocios n
JOIN dim_periodo p ON n.id_periodo = p.id_periodo
WHERE n.recorte_tipo = 'setor'
GROUP BY n.recorte_nome
ORDER BY queda_receita_out DESC;
 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC -  Por porte da empresa (onda de novembro)

-- COMMAND ----------

SELECT n.recorte_nome AS porte, n.pct_emprego_minimo, n.pct_emprego_alto
FROM fato_pesquisa_negocios n
WHERE n.recorte_tipo = 'porte'
ORDER BY n.pct_emprego_minimo DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ** Pergunta 5**— Picos do conflito × atividade
-- MAGIC
-- MAGIC Desvio de cada mês em relação à linha de base pré-guerra (média jan–set/2023)

-- COMMAND ----------

WITH israel AS (
    SELECT p.ano, p.mes, m.taxa_desemprego_sa_pct, m.ausentes_temporarios_sa_mil
    FROM fato_indicador_mensal m
    JOIN dim_periodo p    ON m.id_periodo = p.id_periodo
    JOIN dim_territorio t ON m.id_territorio = t.id_territorio
    WHERE t.nome = 'Israel' AND m.ausentes_temporarios_sa_mil IS NOT NULL
),
base AS (
    SELECT AVG(ausentes_temporarios_sa_mil) AS ausentes_base, AVG(taxa_desemprego_sa_pct) AS desemprego_base
    FROM israel WHERE ano = 2023 AND mes <= 9
)
SELECT i.ano, i.mes,
       i.ausentes_temporarios_sa_mil,
       ROUND((i.ausentes_temporarios_sa_mil / b.ausentes_base - 1) * 100, 1) AS ausentes_desvio_pct,
       i.taxa_desemprego_sa_pct,
       ROUND(i.taxa_desemprego_sa_pct - b.desemprego_base, 2) AS desemprego_desvio_pp
FROM israel i CROSS JOIN base b
ORDER BY i.ano, i.mes;