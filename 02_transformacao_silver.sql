-- Databricks notebook source
-- MAGIC %md
-- MAGIC 02 - Transformação de Dados - Camada Silver
-- MAGIC
-- MAGIC Limpeza e padronização: nomes de território unificados, tipos convertidos, fontes equivalentes unidas, formato comprido pivotado e granularidade semanal agregada para mensal.
-- MAGIC
-- MAGIC Nenhum valor nulo é preenchido artificialmente, porque eles são parte da análise (ver Qualidade de Dados no README).

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS mvp_economia_guerra.silver;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC 1. PIB (anual + trimestral) — união de 3 fontes: 
-- MAGIC
-- MAGIC WDI (anual, combinado) + AHLC trimestral combinado + AHLC por território (linha "Gross Domestic Product" da tabela setorial, que representa o total da economia).
-- MAGIC
-- MAGIC  

-- COMMAND ----------


CREATE OR REPLACE TABLE mvp_economia_guerra.silver.pib_padronizado AS
SELECT
    CASE territorio WHEN 'West Bank and Gaza' THEN 'Cisjordania e Gaza (combinado)' ELSE territorio END AS territorio,
    CAST(ano AS INT) AS ano,
    CAST(NULL AS INT) AS trimestre,
    CAST(variacao_pib_pct AS DECIMAL(6,2)) AS variacao_pib_pct,
    fonte AS nome_fonte, licenca, url_fonte,
    current_timestamp() AS data_transformacao
FROM mvp_economia_guerra.bronze.worldbank_gdp_growth_raw
UNION ALL
SELECT
    CASE territorio WHEN 'Palestine' THEN 'Palestina' ELSE territorio END,
    CAST(ano AS INT), CAST(trimestre AS INT),
    CAST(variacao_pct AS DECIMAL(6,2)),
    fonte, licenca, url_fonte, current_timestamp()
FROM mvp_economia_guerra.bronze.worldbank_ahlc_gdp_combined_quarterly_raw
UNION ALL
SELECT
    CASE territorio WHEN 'West Bank' THEN 'Cisjordania' WHEN 'Gaza Strip' THEN 'Gaza' ELSE territorio END,
    CAST(ano AS INT), CAST(trimestre AS INT),
    CAST(variacao_pct AS DECIMAL(6,2)),
    fonte, licenca, url_fonte, current_timestamp()
FROM mvp_economia_guerra.bronze.worldbank_ahlc_sector_q1_2024_raw
WHERE setor = 'Gross Domestic Product';

SELECT * FROM mvp_economia_guerra.silver.pib_padronizado
ORDER BY territorio, ano, trimestre;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Desemprego anual:
-- MAGIC
-- MAGIC União PCBS + Israel CBS (mesma métrica, padrão ILO/ICLS-19th)
-- MAGIC  

-- COMMAND ----------

CREATE OR REPLACE TABLE mvp_economia_guerra.silver.desemprego_padronizado AS 
SELECT
    CASE territorio WHEN 'West Bank' THEN 'Cisjordania' WHEN 'Gaza Strip' THEN 'Gaza'
        WHEN 'Palestine' THEN 'Palestina' ELSE territorio END AS territorio,
    CAST(ano AS INT) AS ano,
    CAST(taxa_desemprego_pct AS DECIMAL(6,2)) AS taxa_desemprego_pct,
    fonte AS nome_fonte, licenca, url_fonte,
    current_timestamp() AS data_transformacao
FROM mvp_economia_guerra.bronze.pcbs_unemployment_raw
UNION ALL
SELECT territorio, CAST(ano AS INT), CAST(taxa_desemprego_pct AS DECIMAL(6,2)),
       fonte, licenca, url_fonte, current_timestamp()
FROM mvp_economia_guerra.bronze.israel_cbs_unemployment_raw;
 
SELECT * FROM mvp_economia_guerra.silver.desemprego_padronizado 
ORDER BY territorio, ano;
 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC  3. PIB setorial (AHLC)
-- MAGIC  
-- MAGIC  Exclui a linha de PIB total (já tratada na seção 1)

-- COMMAND ----------

CREATE OR REPLACE TABLE mvp_economia_guerra.silver.pib_setorial_padronizado AS 
SELECT 
CASE territorio WHEN 'West Bank' THEN 'Cisjordania' WHEN 'Gaza Strip' THEN 'Gaza' ELSE territorio END AS territorio,
    setor,
    CAST(ano AS INT) AS ano,
    CAST(trimestre AS INT) AS trimestre,
    CAST(variacao_pct AS DECIMAL(6,2)) AS variacao_pct,
    fonte AS nome_fonte, licenca, url_fonte,
    current_timestamp() AS data_transformacao
FROM mvp_economia_guerra.bronze.worldbank_ahlc_sector_q1_2024_raw
WHERE setor <> 'Gross Domestic Product';
 
SELECT * FROM mvp_economia_guerra.silver.pib_setorial_padronizado 
ORDER BY territorio, variacao_pct;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Pesquisa de negócios (CBS 380/2023) — pivot do formato comprido para largo
-- MAGIC
-- MAGIC Agregação condicional (`MAX(CASE WHEN ...)`): cada métrica vira uma coluna; a onda vira ano/mês.

-- COMMAND ----------

CREATE OR REPlACE TABLE mvp_economia_guerra.silver.pesquisa_negocios_padronizada AS 
SELECT
    'Israel' AS territorio,
     2023 AS ano,
    CASE onda WHEN 'outubro_2023' THEN 10 WHEN 'novembro_2023' THEN 11 END AS mes, 
        abrangencia AS recorte_tipo, 
        categoria AS recorte_nome, 
        CAST (MAX(CASE WHEN metrica = 'pct_negocios_emprego_minimo' THEN valor_pct END) AS DECIMAL (5,2)) AS pct_emprego_minimo,
        CAST (MAX(CASE WHEN metrica = 'pct_negocios_emprego_alto_81mais' THEN valor_pct END) AS DECIMAL (5,2)) AS pct_emprego_alto,
        CAST(MAX(CASE WHEN metrica = 'pct_negocios_receita_queda_severa_50mais' THEN valor_pct END)AS DECIMAL (5,2)) AS pct_queda_receita_severa,
        CAST(MAX(CASE WHEN metrica = 'pct_causas_negocios_queda_demanda' THEN valor_pct END) AS DECIMAL(5,2)) AS pct_causa_demanda,
        CAST(MAX(CASE WHEN metrica = 'pct_negocios_causa_falta_trabalhadores'   THEN valor_pct END) AS DECIMAL(5,2)) AS pct_causa_falta_trabalhadores,
        MAX (fonte) AS nome_fonte, MAX (licenca) AS licenca, MAX (url_fonte) AS url_fonte, 
            current_timestamp() AS data_transformacao
FROM mvp_economia_guerra.bronze.israel_cbs_business_survey_raw
GROUP BY onda, abrangencia, categoria;

SELECT * FROM mvp_economia_guerra.silver.pesquisa_negocios_padronizada
ORDER BY recorte_nome, recorte_tipo, mes;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5. Mercado de trabalho mensal de Israel (dessazonalizado)
-- MAGIC
-- MAGIC Usa a série dessazonalizada para evitar que feriados (Pessach em abril, férias em agosto) sejam confundidos com efeito da guerra (ver Outliers no README).
-- MAGIC  

-- COMMAND ----------

CREATE OR REPLACE TABLE mvp_economia_guerra.silver.trabalho_mensal_padronizado AS 
SELECT 
    territorio, 
    CAST (ano AS INT) AS ano, 
    CAST (mes AS INT) AS mes,
    CAST (taxa_desemprego_sa_pct AS DECIMAL (5,2)) AS taxa_desemprego_sa_pct, 
    CAST (ausentes_temporarios_sa_mil AS DECIMAL(8,1)) AS ausentes_temporarios_sa_mil, 
    fonte AS nome_fonte, licenca, url_fonte, 
    current_timestamp() AS data_transformacao
FROM mvp_economia_guerra.bronze.israel_cbs_labour_monthly_raw;

SELECT * FROM mvp_economia_guerra.silver.trabalho_mensal_padronizado
ORDER BY territorio, ano, mes;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 6. Verificações de qualidade (evidências para o README)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 6.1  Completude
-- MAGIC
-- MAGIC Valores nulos de desemprego por território

-- COMMAND ----------

SELECT territorio, COUNT (*) AS linhas, COUNT (taxa_desemprego_pct) AS com_valor, 
    COUNT (*) - COUNT (taxa_desemprego_pct) AS nulas
FROM mvp_economia_guerra.silver.desemprego_padronizado
GROUP BY territorio
ORDER BY territorio;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 6.2 Unicidade 
-- MAGIC
-- MAGIC Não deve haver mais de uma linha por chave natural (resultado esperado: vazio)

-- COMMAND ----------

SELECT 'pib' AS tabela, territorio, ano, trimestre, nome_fonte, COUNT(*) AS n
FROM mvp_economia_guerra.silver.pib_padronizado    
GROUP BY territorio, ano, trimestre, nome_fonte HAVING COUNT(*) > 1
UNION ALL
SELECT 'trabalho_mensal', territorio, ano, mes, nome_fonte, COUNT(*) 
FROM mvp_economia_guerra.silver.trabalho_mensal_padronizado
GROUP BY territorio, ano, mes, nome_fonte HAVING COUNT(*) > 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 6.3 Acurácia 
-- MAGIC
-- MAGIC Valores fora do domínio esperado (resultado esperado: vazio)

-- COMMAND ----------

SELECT 'desemprego fora de 0-100' AS regra, territorio, ano, taxa_desemprego_pct AS valor 
FROM mvp_economia_guerra.silver.desemprego_padronizado
WHERE taxa_desemprego_pct < 0 OR taxa_desemprego_pct > 100
UNION ALL 
SELECT 'variacao setorial abaixo de -100%', territorio, ano, variacao_pct
FROM mvp_economia_guerra.silver.pib_setorial_padronizado 
WHERE variacao_pct < - 100
UNION ALL SELECT 'pib setorial fora de 0-100', recorte_nome, ano, pct_queda_receita_severa
FROM mvp_economia_guerra.silver.pesquisa_negocios_padronizada
WHERE pct_queda_receita_severa < 0 OR pct_queda_receita_severa > 100

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 6.4 Rastreabilidade de Licença 
-- MAGIC
-- MAGIC Cada fonte segue com sua licença de origem

-- COMMAND ----------

SELECT DISTINCT nome_fonte, licenca FROM mvp_economia_guerra.silver.pib_padronizado
UNION SELECT DISTINCT nome_fonte, licenca FROM mvp_economia_guerra.silver.desemprego_padronizado
UNION SELECT DISTINCT nome_fonte, licenca FROM mvp_economia_guerra.silver.pib_setorial_padronizado
UNION SELECT DISTINCT nome_fonte, licenca FROM mvp_economia_guerra.silver.pesquisa_negocios_padronizada
UNION SELECT DISTINCT nome_fonte, licenca FROM mvp_economia_guerra.silver.trabalho_mensal_padronizado;
 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC  7. Catálogo de dados — comentários das tabelas e colunas Silver
-- MAGIC
-- MAGIC Descrição de cada tabela e coluna registrada no Unity Catalog -  catálogo de dados navegável exigido na Etapa 4.3.

-- COMMAND ----------

COMMENT ON TABLE mvp_economia_guerra.silver.pib_padronizado IS 'SILVER: variação do PIB total, anual e trimestral, unindo WDI e relatório do World Bank; territórios padronizados';
ALTER TABLE mvp_economia_guerra.silver.pib_padronizado ALTER COLUMN territorio COMMENT 'Território padronizado (nomes que aparecerão no gold.dim_territorio)';
ALTER TABLE mvp_economia_guerra.silver.pib_padronizado ALTER COLUMN ano COMMENT 'Ano de referência';
ALTER TABLE mvp_economia_guerra.silver.pib_padronizado ALTER COLUMN trimestre COMMENT 'Trimestre; NULL para dado anual'; 
ALTER TABLE mvp_economia_guerra.silver.pib_padronizado ALTER COLUMN variacao_pib_pct COMMENT 'Variação % do PIB';
ALTER TABLE mvp_economia_guerra.silver.pib_padronizado ALTER COLUMN nome_fonte COMMENT 'Nome da fonte (Fk para gold.dim_fonte)';
ALTER TABLE mvp_economia_guerra.silver.pib_padronizado ALTER COLUMN licenca COMMENT 'Licença herdada da camada Bronze)';
ALTER TABLE mvp_economia_guerra.silver.pib_padronizado ALTER COLUMN url_fonte COMMENT 'Link para fonte original'; 
ALTER TABLE mvp_economia_guerra.silver.pib_padronizado ALTER COLUMN data_transformacao COMMENT 'Momento que a linha foi gerada na Silver';

-- COMMAND ----------

COMMENT ON TABLE mvp_economia_guerra.silver.desemprego_padronizado IS 'SILVER: taxa de desemprego anual unindo PCBS e Israel CBS (mesma métrica ILO/ICLS-19th); nulos preservados.';
ALTER TABLE mvp_economia_guerra.silver.desemprego_padronizado ALTER COLUMN territorio COMMENT 'Territorio padronizado';
ALTER TABLE mvp_economia_guerra.silver.desemprego_padronizado ALTER COLUMN ano COMMENT 'Ano de referência';
ALTER TABLE mvp_economia_guerra.silver.desemprego_padronizado ALTER COLUMN taxa_desemprego_pct COMMENT 'Taxa de desemprego em porcentagem; NULL em Gaza/Palestina a partir de 2023.'; 
ALTER TABLE mvp_economia_guerra.silver.desemprego_padronizado ALTER COLUMN nome_fonte COMMENT 'Nome da fonte (Fk para gold.dim_fonte)';
ALTER TABLE mvp_economia_guerra.silver.desemprego_padronizado AlTER COLUMN licenca COMMENT 'Licença de uso herdada da Camada Bronze';
ALTER TABLE mvp_economia_guerra.silver.desemprego_padronizado ALTER COLUMN url_fonte COMMENT 'Link para fonte original'; 
ALTER TABLE mvp_economia_guerra.silver.desemprego_padronizado ALTER COLUMN data_transformacao COMMENT 'Momento que a linha foi gerada na Silver';

-- COMMAND ----------

COMMENT ON TABLE mvp_economia_guerra.silver.pib_setorial_padronizado IS 
'SILVER: variação percentual do PIB por setor (sem a linha de PIB total, já em pib_padronizado).';
ALTER TABLE mvp_economia_guerra.silver.pib_setorial_padronizado 
ALTER COLUMN territorio COMMENT 'Território padronizado: Cisjordania ou Gaza';
ALTER TABLE mvp_economia_guerra.silver.pib_setorial_padronizado
ALTER COLUMN setor COMMENT 'Setor econômico (classificação do World Bank)';
ALTER TABLE mvp_economia_guerra.silver.pib_setorial_padronizado ALTER COLUMN ano COMMENT 'Ano do trimestre';
ALTER TABLE mvp_economia_guerra.silver.pib_setorial_padronizado ALTER COLUMN trimestre COMMENT 'Trimestre de referência';
ALTER TABLE mvp_economia_guerra.silver.pib_setorial_padronizado ALTER COLUMN variacao_pct 
COMMENT 'Variação % contra o mesmo trimestre do ano anterior';
ALTER TABLE mvp_economia_guerra.silver.pib_setorial_padronizado ALTER COLUMN nome_fonte COMMENT 'Nome da fonte (FK lógica para gold.dim_fonte)';
ALTER TABLE mvp_economia_guerra.silver.pib_setorial_padronizado ALTER COLUMN licenca COMMENT 
'Licença de uso herdada da Bronze';
ALTER TABLE mvp_economia_guerra.silver.pib_setorial_padronizado ALTER COLUMN url_fonte
 COMMENT 'Link para a fonte original';
ALTER TABLE mvp_economia_guerra.silver.pib_setorial_padronizado ALTER COLUMN
 data_transformacao COMMENT 'Momento em que a linha foi gerada na Silver';
 

-- COMMAND ----------

COMMENT ON TABLE mvp_economia_guerra.silver.pesquisa_negocios_padronizada IS 'SILVER: pesquisa de negócios da organização governamental de pesquisa estátistica de Israel, Israel Central Bureau of Statistics (CBS), pivotada para formato largo (uma coluna por métrica).';
ALTER TABLE mvp_economia_guerra.silver.pesquisa_negocios_padronizada ALTER COLUMN territorio COMMENT 'Territorio: Israel';
ALTER TABLE mvp_economia_guerra.silver.pesquisa_negocios_padronizada ALTER COLUMN ano COMMENT 'Ano da onda (2023, que foi o ano da primeira onda de pesquisas estatísticas)';
ALTER TABLE mvp_economia_guerra.silver.pesquisa_negocios_padronizada ALTER COLUMN mes COMMENT 'Mês da onda: 10 outubro ou 11 (novembro)';
ALTER TABLE mvp_economia_guerra.silver.pesquisa_negocios_padronizada ALTER COLUMN recorte_tipo COMMENT 'Tipo de recorte: nacional, distrito, setor ou porte'; 
ALTER TABLE mvp_economia_guerra.silver.pesquisa_negocios_padronizada ALTER COLUMN recorte_nome COMMENT 'Valor do recorte';
ALTER TABLE mvp_economia_guerra.silver.pesquisa_negocios_padronizada ALTER COLUMN pct_emprego_minimo COMMENT 'percentual de negócios que ainda têm até 20% da equipe pré-guerra ativa';
ALTER TABLE mvp_economia_guerra.silver.pesquisa_negocios_padronizada ALTER COLUMN pct_emprego_alto COMMENT 'percentual de negócios que ainda têm 81% ou mais da equipe ativa';
ALTER TABLE mvp_economia_guerra.silver.pesquisa_negocios_padronizada ALTER COLUMN pct_queda_receita_severa COMMENT 'percentual de negócios que possuem queda de receita esperada acima de 50%';
ALTER TABLE mvp_economia_guerra.silver.pesquisa_negocios_padronizada ALTER COLUMN pct_causa_demanda COMMENT 'percentual que aponta a falta de demanda como principal causa da queda de receita';
ALTER TABLE mvp_economia_guerra.silver.pesquisa_negocios_padronizada ALTER COLUMN pct_causa_falta_trabalhadores COMMENT 'percentual que aponta a falta de trabalhadores como principal causa da queda de receita';
ALTER TABLE mvp_economia_guerra.silver.pesquisa_negocios_padronizada ALTER COLUMN nome_fonte COMMENT 'Nome da fonte (FK lógica para gold.dim_fonte)';
ALTER TABLE mvp_economia_guerra.silver.pesquisa_negocios_padronizada ALTER COLUMN licenca COMMENT 'Licença de uso herdada da Bronze';
ALTER TABLE mvp_economia_guerra.silver.pesquisa_negocios_padronizada ALTER COLUMN url_fonte
 COMMENT 'Link para a fonte original';
ALTER TABLE mvp_economia_guerra.silver.pesquisa_negocios_padronizada ALTER COLUMN
 data_transformacao COMMENT 'Momento em que a linha foi gerada na Silver';


-- COMMAND ----------

COMMENT ON TABLE mvp_economia_guerra.silver.trabalho_mensal_padronizado IS 'SILVER: indicadores mensais do mercado de trabalho israelense que foram dessazonalizados.';
ALTER TABLE mvp_economia_guerra.silver.trabalho_mensal_padronizado ALTER COLUMN territorio COMMENT 'Territorio: Israel';
ALTER TABLE mvp_economia_guerra.silver.trabalho_mensal_padronizado ALTER COLUMN ano COMMENT 'Ano de referência';
ALTER TABLE mvp_economia_guerra.silver.trabalho_mensal_padronizado ALTER COLUMN mes COMMENT 'Mês de referência';
ALTER TABLE mvp_economia_guerra.silver.trabalho_mensal_padronizado ALTER COLUMN taxa_desemprego_sa_pct COMMENT 'Taxa de desemprego dessazonalizada em percentual';
AlTER TABLE mvp_economia_guerra.silver.trabalho_mensal_padronizado ALTER COLUMN ausentes_temporarios_sa_mil COMMENT 'Trabalhadores temporariamente ausentes, milhares, dessasonalizados';
ALTER TABLE mvp_economia_guerra.silver.trabalho_mensal_padronizado ALTER COLUMN nome_fonte COMMENT 'Nome da fonte (FK lógica para gold.dim_fonte)';
ALTER TABLE mvp_economia_guerra.silver.trabalho_mensal_padronizado ALTER COLUMN licenca COMMENT 'Licença de uso herdada da Bronze';
ALTER TABLE mvp_economia_guerra.silver.trabalho_mensal_padronizado ALTER COLUMN url_fonte COMMENT 'Link para a fonte original';
ALTER TABLE mvp_economia_guerra.silver.trabalho_mensal_padronizado ALTER COLUMN data_transformacao COMMENT 'Momento em que a linha foi gerada na Silver';

-- COMMAND ----------

SHOW TABLES IN mvp_economia_guerra.silver;