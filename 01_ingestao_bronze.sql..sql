-- Databricks notebook source
-- MAGIC %md
-- MAGIC 01 - Ingestão de dados - Camada Bronze
-- MAGIC
-- MAGIC **Camada Bronze:** dados brutos, sem limpeza ou alteração de conteúdo. Os registros JSON já carregam "fonte", "licença" e "url". 
-- MAGIC
-- MAGIC  **Decisão de tipagem:** os tipos são inferidos automaticamente (`mergeSchema = true`) já na camada Bronze em vez da prática alternativa de manter tudo como STRING nesta camada e só tipar na Silver. Optou-se por isso porque os JSONs de origem são pequenos e foram validados linha a linha antes do upload (melhor elaborado no README).

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS mvp_economia_guerra;


-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS mvp_economia_guerra.bronze;

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS mvp_economia_guerra.bronze.raw_files; 

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC Em raw_files foi feito o upload de todos os arquivos de dados em JSON.
-- MAGIC
-- MAGIC Tabela de referência: 
-- MAGIC
-- MAGIC      Registra como dado a mesma informação da tabela de "fontes de dados brutos" solicitada na etapa 4.1.
-- MAGIC
-- MAGIC

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS mvp_economia_guerra.bronze.fonte_dados_brutos (
    ordem INT,
    fonte STRING,
    `território` STRING,
    `formato_original` STRING,
    conteudo STRING,
    `status_de_ingestão` STRING
);
INSERT INTO mvp_economia_guerra.bronze.fonte_dados_brutos
VALUES (1, 'World Bank WDI - GDP growth (anual %)', 'Israel; Cisjordania e Gaza (combinados)', 'Tabela web (Data360)', 'Crescimento anual do PIB - 1961-2025', 'Ingerida'),
(2, 'World Bank AHLC - Economic Monitoring Report (set/2024)', 'Cisjordania; Gaza; Palestina (combinado)', 'PDF', 'PIB por setor (Q1-2024 vs Q1-2023) e PIB trimestral combinado', 'Ingerida'),
(3, 'PCBS - Labour Force Survey', 'Cisjordania; Gaza; Palestina (combinado)', 'Tabela web', 'Taxa de desemprego anual (padrao ILO/ICLS-19th)', 'Ingerida'),
(4, 'Israel CBS - Table 1.1 (Labour force characteristics)', 'Israel', 'PDF', 'Taxa de desemprego anual e mensal (original, dessazonalizada, tendencia)', 'Ingerida'),
(5, 'Israel CBS - Table 1.4 (Employed persons, by extent of work)', 'Israel', 'PDF', 'Ocupados em tempo integral, parcial e temporariamente ausentes, mensal', 'Ingerida'),
(6, 'Israel CBS - Comunicado 380/2023 (Struggles of Business During Swords of Iron)', 'Israel (nacional, distritos, setores, porte)', 'PDF (hebraico)', 'Percentual de negocios em níveis de empregabilidade e com queda severa de receita, ondas out/nov 2023', 'Ingerida'),
(7, 'Taub Center - The Labor Market in Israel in 2024 in the Shadow of War', 'Israel','PDF', 'Emprego por setor, reservistas, evacuados', 'Nao ingerida - citada apenas em prosa na Analise (sem licenca aberta declarada)');
SELECT * FROM mvp_economia_guerra.bronze.fontes_dados_brutos ORDER BY ordem;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC  1. World Bank WDI — GDP growth (annual %) · Licença CC BY 4.0 em JSON

-- COMMAND ----------

DROP TABLE IF EXISTS mvp_economia_guerra.bronze.worldbank_gdp_growth_raw;

CREATE TABLE mvp_economia_guerra.bronze.worldbank_gdp_growth_raw;

COPY INTO mvp_economia_guerra.bronze.worldbank_gdp_growth_raw
FROM '/Volumes/mvp_economia_guerra/bronze/raw_files/worldbank_gdp_growth.json'
FILEFORMAT = JSON
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');
SELECT * FROM mvp_economia_guerra.bronze.worldbank_gdp_growth_raw ORDER BY territorio, ano;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. PCBS — Labour Force Survey (desemprego anual) · Licença CC BY 4.0 - Resutado `null` de Gaza e Palestina a partir de 2023 são mantidos como parte da análise e não erro.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS mvp_economia_guerra.bronze.pcbs_unemployment_raw;
COPY INTO mvp_economia_guerra.bronze.pcbs_unemployment_raw FROM '/Volumes/mvp_economia_guerra/bronze/raw_files/pcbs_unemployment.json'
FILEFORMAT = JSON
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');
SElECT * FROM mvp_economia_guerra.bronze.pcbs_unemployment_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Israel CBS — Table 1.1 (desemprego anual) - Licença CBS 'Open License'

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS mvp_economia_guerra.bronze.israel_cbs_unemployment_raw;
COPY INTO mvp_economia_guerra.bronze.israel_cbs_unemployment_raw FROM '/Volumes/mvp_economia_guerra/bronze/raw_files/israel_cbs_unemployment.json'
FILEFORMAT = JSON
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');
SElECT * FROM mvp_economia_guerra.bronze.israel_cbs_unemployment_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. World Bank AHLC (Sept 2024) — variação setorial do PIB, Q1-2024 · Licença CC BY 3.0 IGO
-- MAGIC
-- MAGIC Obs: Licença diferente do WDI — cada produto do World Bank tem a sua própria licença. 

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS mvp_economia_guerra.bronze.worldbank_ahlc_sector_q1_2024_raw; 
COPY INTO mvp_economia_guerra.bronze.worldbank_ahlc_sector_q1_2024_raw FROM '/Volumes/mvp_economia_guerra/bronze/raw_files/worldbank_ahlc_sector_q1_2024.json'
FILEFORMAT = JSON
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');
SElECT * FROM mvp_economia_guerra.bronze.worldbank_ahlc_sector_q1_2024_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5. World Bank AHLC (Sept 2024) — PIB trimestral combinado (Palestina) - Licença CC BY 3.0 IGO

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS mvp_economia_guerra.bronze.worldbank_ahlc_gdp_combined_quarterly_raw;
COPY INTO mvp_economia_guerra.bronze.worldbank_ahlc_gdp_combined_quarterly_raw FROM '/Volumes/mvp_economia_guerra/bronze/raw_files/worldbank_ahlc_gdp_combined_quarterly.json'
FILEFORMAT = JSON
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');
SElECT * FROM mvp_economia_guerra.bronze.worldbank_ahlc_gdp_combined_quarterly_raw;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC 6. Israel CBS — Comunicado 380/2023 (pesquisa emergencial de negócios) · Licença CBS 'Open License'. Esse dado é um dado ruim. 

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS mvp_economia_guerra.bronze.israel_cbs_business_survey_raw;
COPY INTO mvp_economia_guerra.bronze.israel_cbs_business_survey_raw FROM '/Volumes/mvp_economia_guerra/bronze/raw_files/israel_cbs_business_survey.json'
FILEFORMAT = JSON
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');
SElECT * FROM mvp_economia_guerra.bronze.israel_cbs_business_survey_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 7. Israel CBS — Tables 1.1 e 1.4· Licença CBS 'Open License'. Taxa de desemprego e trabalhadores temporariamente ausentes (milhares), jan/2023–dez/2024.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS mvp_economia_guerra.bronze.israel_cbs_labour_monthly_raw; 
COPY INTO mvp_economia_guerra.bronze.israel_cbs_labour_monthly_raw FROM '/Volumes/mvp_economia_guerra/bronze/raw_files/israel_cbs_labour_monthly.json'
FILEFORMAT = JSON
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');
SElECT * FROM mvp_economia_guerra.bronze.israel_cbs_labour_monthly_raw;

-- COMMAND ----------

-- MAGIC
-- MAGIC  %md
-- MAGIC  Análise exploratória da Camada Bronze (antes de qualquer transformação dos dados brutos).
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Volume
-- MAGIC
-- MAGIC Registro de volume de dados por tabela: 

-- COMMAND ----------

SELECT 'worldbank_gdp_growth_raw' AS tabala, COUNT (*) AS registros FROM mvp_economia_guerra.bronze.worldbank_gdp_growth_raw
UNION ALL SELECT 'pcbs_unemployment_raw' AS tabela, COUNT (*) AS registros FROM mvp_economia_guerra.bronze.pcbs_unemployment_raw
UNION ALL SELECT 'israel_cbs_unemployment_raw' AS tabela, COUNT (*) AS registros FROM mvp_economia_guerra.bronze.israel_cbs_unemployment_raw
UNION ALL SELECT 'worldbank_ahlc_sector_q1_2024_raw' AS tabela, COUNT (*) AS registros FROM mvp_economia_guerra.bronze.worldbank_ahlc_sector_q1_2024_raw     
UNION ALL SELECT 'worldbank_gdp_combined_quarterly_raw' AS tabela, COUNT (*) AS registros FROM mvp_economia_guerra.bronze.worldbank_ahlc_gdp_combined_quarterly_raw
UNION ALL SELECT 'israel_cbs_business_survey_raw' AS tabela, COUNT (*) AS registros FROM mvp_economia_guerra.bronze.israel_cbs_business_survey_raw
UNION ALL SELECT 'israel_cbs_labour_monthly_raw' AS tabela, COUNT (*) AS registros FROM mvp_economia_guerra.bronze.israel_cbs_labour_monthly_raw  


-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Consistência 
-- MAGIC
-- MAGIC São esperadas cinco grafias diferentes. Será feito o uso do CASE WHEN na camada Silver.

-- COMMAND ----------

SELECT DISTINCT territorio FROM mvp_economia_guerra.bronze.worldbank_gdp_growth_raw
UNION SELECT DISTINCT territorio FROM mvp_economia_guerra.bronze.worldbank_ahlc_sector_q1_2024_raw
UNION SELECT DISTINCT territorio FROM mvp_economia_guerra.bronze.worldbank_ahlc_gdp_combined_quarterly_raw
UNION SELECT DISTINCT territorio FROM mvp_economia_guerra.bronze.pcbs_unemployment_raw
UNION SELECT DISTINCT territorio FROM mvp_economia_guerra.bronze.israel_cbs_unemployment_raw
UNION SELECT DISTINCT territorio FROM mvp_economia_guerra.bronze.israel_cbs_labour_monthly_raw
israel_cbs_business_survey_raw
ORDER BY territorio;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Completude 
-- MAGIC
-- MAGIC São esperados seis nulos na tabela pcbs_unemployment_raw, referente aos dados faltantes de Gaza e Palestina para os anos de 2023, 2024 e 2025 e nenhum dado nulo nas outras tabelas. 
-- MAGIC
-- MAGIC

-- COMMAND ----------

SELECT 'worldbank_gdp_growth_raw' AS tabela, COUNT (*) AS linhas, COUNT(*) - COUNT(variacao_pib_pct) AS nulos FROM mvp_economia_guerra.bronze.worldbank_gdp_growth_raw
UNION ALL SELECT 'pcbs_unemployment_raw' AS tabela, COUNT (*) AS linhas, COUNT(*) - COUNT(taxa_desemprego_pct) AS nulos FROM mvp_economia_guerra.bronze.pcbs_unemployment_raw
UNION ALL SELECT 'worldbank_ahlc_sector_q1_2024_raw' AS tabela, COUNT (*) AS linhas, COUNT (*) - COUNT(variacao_pct) AS nulos FROM mvp_economia_guerra.bronze.worldbank_ahlc_sector_q1_2024_raw
UNION ALL SELECT 'worldbank_ahlc_gdp_combined_quarterly_raw'AS tabela, COUNT (*) AS linhas, COUNT (*) - COUNT(variacao_pct) AS NULOS FROM mvp_economia_guerra.bronze.worldbank_ahlc_gdp_combined_quarterly_raw
UNION ALL SELECT 'israel_cbs_labour_monthly_raw'AS tabela, COUNT (*) AS linhas, COUNT (*) - COUNT (taxa_desemprego_sa_pct) AS NULOS FROM mvp_economia_guerra.bronze.israel_cbs_labour_monthly_raw
UNION ALL SELECT 'israel_cbs_unemployment' AS tabela, COUNT (*) AS linhas, COUNT (*) - COUNT (taxa_desemprego_pct) AS NULOS FROM mvp_economia_guerra.bronze.israel_cbs_unemployment_raw
UNION ALL SELECT 'israel_cbs_business_suvery_raw' AS tabela, COUNT (*) AS linhas, COUNT (*) - COUNT (valor_pct) AS NULOS FROM mvp_economia_guerra.bronze.israel_cbs_business_survey_raw          

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Para evidenciar que se trata de uma lacuna de informação necessária para análise e não um erro, foi gerada a tabela com os nulos do pcbs_unemployment_raw. Os resultados esperados são Gaza Strio e Palestina, anos 2023, 2024 e 2025. 

-- COMMAND ----------

SELECT territorio, ano, taxa_desemprego_pct
FROM mvp_economia_guerra.bronze.pcbs_unemployment_raw
WHERE taxa_desemprego_pct IS NULL
ORDER BY territorio, ano

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Acurácia e Outliers
-- MAGIC
-- MAGIC Variações entre -99 e -10 nos valores setorias são esperadas e as quedas extremas nos valores de Gaza não são erro e correspondem aos dados brutos reais. 
-- MAGIC
-- MAGIC Se tratando do desemprego, espera-se que todos os dados fiquem entre 0-100% e não ultrapassem esse valor nas métricas percentuais.
-- MAGIC
-- MAGIC 'Ausentes Israel mensal" é uma contagem em milhares de pessoas, não um percentual por isso seus valores (centenas) são de outra ordem de grandeza e não devem ser comparados diretamente com as demais linhas desta consulta. Resultado esperado: ausentes entre 256,9 (mil) e 683,0 (mil). 

-- COMMAND ----------

SELECT 'PIB anual (%)' AS metrica, MIN(variacao_pib_pct) AS mininimo, MAX(variacao_pib_pct) AS maximo FROM mvp_economia_guerra.bronze.worldbank_gdp_growth_raw
UNION ALL SELECT 'PIB setorial (%)' AS metrica, MIN(variacao_pct) AS minimo, MAX(variacao_pct) AS maximo FROM mvp_economia_guerra.bronze.worldbank_ahlc_sector_q1_2024_raw
UNION ALL SELECT 'Desemprego PCBS (%)', MIN(taxa_desemprego_pct), MAX(taxa_desemprego_pct) FROM mvp_economia_guerra.bronze.pcbs_unemployment_raw
UNION ALL SELECT 'Desemprego Israel mensal (%)', MIN(taxa_desemprego_sa_pct), MAX(taxa_desemprego_sa_pct) FROM mvp_economia_guerra.bronze.israel_cbs_labour_monthly_raw
UNION ALL SELECT 'Ausentes Israel mensal (mil)', MIN(ausentes_temporarios_sa_mil), MAX(ausentes_temporarios_sa_mil) FROM mvp_economia_guerra.bronze.israel_cbs_labour_monthly_raw
UNION ALL SELECT 'Pesquisa de negócios (%)', MIN(valor_pct), MAX(valor_pct) FROM mvp_economia_guerra.bronze.israel_cbs_business_survey_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC A tabela worldbank_ahlc_sector_raw tem 22 linhas, 10 setores × 2 territórios (Cisjordânia, Gaza) mais 1 linha de "Gross Domestic Product" (PIB total) por território = 2 linhas extras
-- MAGIC
-- MAGIC Na camada Silver essa linha de PIB total é separada e vai para pib_padronizado, não para pib_setorial_padronizado, junto com as outras 20 linhas. 

-- COMMAND ----------

SELECT CASE WHEN setor = 'Gross Domestic Product' THEN 'PIB total (saí da tabela setorial)'
    ELSE 'setores (permanecem)' END AS destino, 
    COUNT (*) AS linhas 
FROM mvp_economia_guerra.bronze.worldbank_ahlc_sector_q1_2024_raw
GROUP BY 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC A tabela israel_cbs_business_survey_raw está  organizada de modo comprido em sua formma bruta. Na camada Silver, vai ser feita a transformação para o formato largo (pivot)

-- COMMAND ----------

SELECT metrica, onda, COUNT (*) AS linhas 
FROM mvp_economia_guerra.bronze.israel_cbs_business_survey_raw
GROUP BY metrica, onda
ORDER BY metrica, onda

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Catálogo de Dados - comentários das tabelas e colunas Bronze.
-- MAGIC
-- MAGIC Descrição de cada tabela e coluna registrada no Unity Catalog.
-- MAGIC

-- COMMAND ----------

COMMENT ON TABLE mvp_economia_guerra.bronze.worldbank_gdp_growth_raw IS 'Bronze: crescimento anual do PIB (World Bank WDI, NY.GDP.MKTP.KD.ZG), como publicado. Licença CC BY 4.0.';
ALTER TABLE mvp_economia_guerra.bronze.worldbank_gdp_growth_raw ALTER COLUMN territorio COMMENT 'Território como nomeado pela fonte: Israel ou West Bank and Gaza';
ALTER TABLE mvp_economia_guerra.bronze.worldbank_gdp_growth_raw ALTER COLUMN ano COMMENT 'Ano de referência (2020-2025)';
ALTER TABLE mvp_economia_guerra.bronze.worldbank_gdp_growth_raw ALTER COLUMN variacao_pib_pct COMMENT 'Crescimento real do PIB em % sobre o ano anterior';
ALTER TABLE mvp_economia_guerra.bronze.worldbank_gdp_growth_raw ALTER COLUMN fonte COMMENT 'Nome da fonte que publicou o dado';
ALTER TABLE mvp_economia_guerra.bronze.worldbank_gdp_growth_raw ALTER COLUMN licenca COMMENT 'Licença de uso da fonte de origem';
ALTER TABLE mvp_economia_guerra.bronze.worldbank_gdp_growth_raw ALTER COLUMN url_fonte COMMENT 'Link para a fonte original';

-- COMMAND ----------

COMMENT ON TABLE mvp_economia_guerra.bronze.worldbank_ahlc_sector_q1_2024_raw IS 'Bronze: variação % do PIB por setor, Q1-2024 vs Q1-2023 (World Bank, relatório de set/2024, Table 1). Licença CC BY 3.0 IGO.';
ALTER TABLE mvp_economia_guerra.bronze.worldbank_ahlc_sector_q1_2024_raw ALTER COLUMN territorio COMMENT 'Território como nomeado pela fonte: West Bank ou Gaza Strip';
ALTER TABLE mvp_economia_guerra.bronze.worldbank_ahlc_sector_q1_2024_raw ALTER COLUMN setor COMMENT 'Setor econômico, incluindo a linha Gross Domestic Product (total)';
ALTER TABLE mvp_economia_guerra.bronze.worldbank_ahlc_sector_q1_2024_raw ALTER COLUMN ano COMMENT 'Ano do trimestre de referência';
ALTER TABLE mvp_economia_guerra.bronze.worldbank_ahlc_sector_q1_2024_raw ALTER COLUMN trimestre COMMENT 'Trimestre de referência';
ALTER TABLE mvp_economia_guerra.bronze.worldbank_ahlc_sector_q1_2024_raw ALTER COLUMN variacao_pct COMMENT 'Variação % contra o mesmo trimestre do ano anterior';
ALTER TABLE mvp_economia_guerra.bronze.worldbank_ahlc_sector_q1_2024_raw ALTER COLUMN fonte COMMENT 'Nome da fonte que publicou o dado';
ALTER TABLE mvp_economia_guerra.bronze.worldbank_ahlc_sector_q1_2024_raw ALTER COLUMN licenca COMMENT 'Licença de uso da fonte de origem';
ALTER TABLE mvp_economia_guerra.bronze.worldbank_ahlc_sector_q1_2024_raw ALTER COLUMN url_fonte COMMENT 'Link para a fonte original';

-- COMMAND ----------

COMMENT ON TABLE mvp_economia_guerra.bronze.worldbank_ahlc_gdp_combined_quarterly_raw IS 'Bronze: variação % trimestral do PIB da Palestina (combinado), World Bank set/2024. Licença CC BY 3.0 IGO.';
ALTER TABLE mvp_economia_guerra.bronze.worldbank_ahlc_gdp_combined_quarterly_raw ALTER COLUMN territorio COMMENT 'Território: Palestine (Cisjordânia e Gaza combinados)';
ALTER TABLE mvp_economia_guerra.bronze.worldbank_ahlc_gdp_combined_quarterly_raw ALTER COLUMN ano COMMENT 'Ano de referência';
ALTER TABLE mvp_economia_guerra.bronze.worldbank_ahlc_gdp_combined_quarterly_raw ALTER COLUMN trimestre COMMENT 'Trimestre de referência';
ALTER TABLE mvp_economia_guerra.bronze.worldbank_ahlc_gdp_combined_quarterly_raw ALTER COLUMN variacao_pct COMMENT 'Variação % contra o mesmo trimestre do ano anterior';
ALTER TABLE mvp_economia_guerra.bronze.worldbank_ahlc_gdp_combined_quarterly_raw ALTER COLUMN fonte COMMENT 'Nome da fonte que publicou o dado';
ALTER TABLE mvp_economia_guerra.bronze.worldbank_ahlc_gdp_combined_quarterly_raw ALTER COLUMN licenca COMMENT 'Licença de uso da fonte de origem';
ALTER TABLE mvp_economia_guerra.bronze.worldbank_ahlc_gdp_combined_quarterly_raw ALTER COLUMN url_fonte COMMENT 'Link para a fonte original';
 

-- COMMAND ----------

COMMENT ON TABLE mvp_economia_guerra.bronze.pcbs_unemployment_raw IS 'Bronze: taxa de desemprego anual palestina (PCBS Labour Force Survey, ICLS-19th). Licença CC BY 4.0.';
ALTER TABLE mvp_economia_guerra.bronze.pcbs_unemployment_raw ALTER COLUMN territorio COMMENT 'Território como nomeado pela fonte: West Bank, Gaza Strip ou Palestine';
ALTER TABLE mvp_economia_guerra.bronze.pcbs_unemployment_raw ALTER COLUMN ano COMMENT 'Ano de referência (2020-2025)';
ALTER TABLE mvp_economia_guerra.bronze.pcbs_unemployment_raw ALTER COLUMN taxa_desemprego_pct COMMENT 'Taxa de desemprego em %; NULL quando a fonte não publicou (Gaza a partir de 2023)';
ALTER TABLE mvp_economia_guerra.bronze.pcbs_unemployment_raw ALTER COLUMN fonte COMMENT 'Nome da fonte que publicou o dado';
ALTER TABLE mvp_economia_guerra.bronze.pcbs_unemployment_raw ALTER COLUMN licenca COMMENT 'Licença de uso da fonte de origem';
ALTER TABLE mvp_economia_guerra.bronze.pcbs_unemployment_raw ALTER COLUMN url_fonte COMMENT 'Link para a fonte original';

-- COMMAND ----------

COMMENT ON TABLE mvp_economia_guerra.bronze.israel_cbs_unemployment_raw IS 'Bronze: taxa de desemprego anual de Israel (Israel CBS, Table 1.1). CBS Open License.';
ALTER TABLE mvp_economia_guerra.bronze.israel_cbs_unemployment_raw ALTER COLUMN territorio COMMENT 'Território: Israel';
ALTER TABLE mvp_economia_guerra.bronze.israel_cbs_unemployment_raw ALTER COLUMN ano COMMENT 'Ano de referência (2023-2024)';
ALTER TABLE mvp_economia_guerra.bronze.israel_cbs_unemployment_raw ALTER COLUMN taxa_desemprego_pct COMMENT 'Taxa de desemprego média anual em %';
ALTER TABLE mvp_economia_guerra.bronze.israel_cbs_unemployment_raw ALTER COLUMN fonte COMMENT 'Nome da fonte que publicou o dado';
ALTER TABLE mvp_economia_guerra.bronze.israel_cbs_unemployment_raw ALTER COLUMN licenca COMMENT 'Licença de uso da fonte de origem';
ALTER TABLE mvp_economia_guerra.bronze.israel_cbs_unemployment_raw ALTER COLUMN url_fonte COMMENT 'Link para a fonte original';

-- COMMAND ----------

COMMENT ON TABLE mvp_economia_guerra.bronze.israel_cbs_labour_monthly_raw IS 'Bronze: indicadores mensais dessazonalizados de Israel (Israel CBS, Tables 1.1 e 1.4). CBS Open License.';
ALTER TABLE mvp_economia_guerra.bronze.israel_cbs_labour_monthly_raw ALTER COLUMN territorio COMMENT 'Território: Israel';
ALTER TABLE mvp_economia_guerra.bronze.israel_cbs_labour_monthly_raw ALTER COLUMN ano COMMENT 'Ano de referência';
ALTER TABLE mvp_economia_guerra.bronze.israel_cbs_labour_monthly_raw ALTER COLUMN mes COMMENT 'Mês de referência (1-12)';
ALTER TABLE mvp_economia_guerra.bronze.israel_cbs_labour_monthly_raw ALTER COLUMN taxa_desemprego_sa_pct COMMENT 'Taxa de desemprego dessazonalizada em %';
ALTER TABLE mvp_economia_guerra.bronze.israel_cbs_labour_monthly_raw ALTER COLUMN ausentes_temporarios_sa_mil COMMENT 'Trabalhadores temporariamente ausentes, em milhares, dessazonalizado';
ALTER TABLE mvp_economia_guerra.bronze.israel_cbs_labour_monthly_raw ALTER COLUMN fonte COMMENT 'Nome da fonte que publicou o dado';
ALTER TABLE mvp_economia_guerra.bronze.israel_cbs_labour_monthly_raw ALTER COLUMN licenca COMMENT 'Licença de uso da fonte de origem';
ALTER TABLE mvp_economia_guerra.bronze.israel_cbs_labour_monthly_raw ALTER COLUMN url_fonte COMMENT 'Link para a fonte original';
ALTER TABLE mvp_economia_guerra.bronze.israel_cbs_labour_monthly_raw ALTER COLUMN documento_origem COMMENT 'Tabela e arquivo do CBS de onde o valor foi transcrito';

-- COMMAND ----------

COMMENT ON TABLE mvp_economia_guerra.bronze.israel_cbs_business_survey_raw IS 'Bronze: pesquisa emergencial de negócios durante a guerra (Israel CBS, comunicado 380/2023), formato longo. CBS Open License.';
ALTER TABLE mvp_economia_guerra.bronze.israel_cbs_business_survey_raw ALTER COLUMN metrica COMMENT 'Métrica medida (ex.: pct_negocios_emprego_minimo)';
ALTER TABLE mvp_economia_guerra.bronze.israel_cbs_business_survey_raw ALTER COLUMN abrangencia COMMENT 'Tipo de recorte: nacional, distrito, setor ou porte';
ALTER TABLE mvp_economia_guerra.bronze.israel_cbs_business_survey_raw ALTER COLUMN categoria COMMENT 'Valor do recorte (ex.: Sul, Construcao, 5_a_10)';
ALTER TABLE mvp_economia_guerra.bronze.israel_cbs_business_survey_raw ALTER COLUMN onda COMMENT 'Onda da pesquisa: outubro_2023 ou novembro_2023';
ALTER TABLE mvp_economia_guerra.bronze.israel_cbs_business_survey_raw ALTER COLUMN valor_pct COMMENT 'Percentual de negócios na condição medida (0-100)';
ALTER TABLE mvp_economia_guerra.bronze.israel_cbs_business_survey_raw ALTER COLUMN fonte COMMENT 'Nome da fonte que publicou o dado';
ALTER TABLE mvp_economia_guerra.bronze.israel_cbs_business_survey_raw ALTER COLUMN licenca COMMENT 'Licença de uso da fonte de origem';
ALTER TABLE mvp_economia_guerra.bronze.israel_cbs_business_survey_raw ALTER COLUMN url_fonte COMMENT 'Link para a fonte original';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Conferência:

-- COMMAND ----------

SHOW TABLES IN mvp_economia_guerra.bronze;