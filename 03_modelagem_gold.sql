-- Databricks notebook source
-- MAGIC %md
-- MAGIC  03 - Modelagem Gold (constelação de fatos)
-- MAGIC
-- MAGIC Modelo dimensional com 4 dimensões compartilhadas e 3 tabelas fato, cada uma com seu grão:
-- MAGIC
-- MAGIC | Tabela fato | Grão | Perguntas |
-- MAGIC |---|---|---|
-- MAGIC | `fato_atividade_economica` | território × período (ano/trimestre) × setor × fonte | 1, 2, 3 |
-- MAGIC | `fato_pesquisa_negocios` | onda (mês) × recorte (distrito/setor/porte) | 4 |
-- MAGIC | `fato_indicador_mensal` | território × mês × fonte | 5 |
-- MAGIC
-- MAGIC Separar os fatos por grão evita misturar granularidades incompatíveis numa tabela só (o que geraria colunas quase sempre nulas e somas incorretas). Essa escolha foi feita porque, mesmo depois dos nomes terem sido padronizados (território, fonte), os dados continuam tendo grãos diferentes. Nesse caso, não é possível padronizar o grão de uma fonte para combinar com o de outra sem perder informação real; por exemplo, forçar todos os dados para a categoria "mensal" inventaria meses para o dado que é só trimestral.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS mvp_economia_guerra.gold

-- COMMAND ----------

DROP TABLE IF EXISTS mvp_economia_guerra.gold.fato_atividade_economica; 
DROP TABLE IF EXISTS mvp_economia_guerra.gold.fato_pesquisa_negocios;
DROP TABLE IF EXISTS mvp_economia_guerra.gold.fato_indicador_mensal;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Dimensões**

-- COMMAND ----------

CREATE OR REPLACE TABLE mvp_economia_guerra.gold.dim_territorio (
    id_territorio BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY, 
    nome STRING NOT NULL, 
    `regiao` STRING,
    CONSTRAINT pk_territorio PRIMARY KEY (id_territorio) 
);

INSERT INTO mvp_economia_guerra.gold.dim_territorio (nome, `regiao`) 
VALUES 
    ('Israel', 'Oriente Medio'),
    ('Cisjordania', 'Oriente Medio'),
    ('Gaza', 'Oriente Medio'),
    ('Cisjordania e Gaza (combinados)', 'Oriente Medio'),
    ('Palestina', 'Oriente Medio');


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Granularidade mista: anual (trimestre e mês NULL), trimestral (mês NULL) e mensal
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE mvp_economia_guerra.gold.dim_periodo (
    id_periodo BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY, 
    ano INT NOT NULL, 
    trimestre INT,
    mes INT,
    CONSTRAINT pk_periodo PRIMARY KEY (id_periodo)
);
INSERT INTO mvp_economia_guerra.gold.dim_periodo (ano, trimestre, mes)
VALUES 
    (2020, NULL, NULL), (2021, NULL, NULL), (2022, NULL, NULL),
    (2023, NULL, NULL), (2024, NULL, NULL), (2025, NULL, NULL), 
    (2024, 4, NULL), (2024, 1, NULL);

-- meses de jan/2023 a dez/2024, gerados com sequence + explode

INSERT INTO mvp_economia_guerra.gold.dim_periodo (ano, trimestre, mes)
SELECT year(d), quarter(d), month(d)
FROM (SELECT explode(sequence(DATE'2023-01-01', DATE '2024-12-01', INTERVAL 1 MONTH )) AS d); 

SELECT * FROM mvp_economia_guerra.gold.dim_periodo  
ORDER BY ano, trimestre NULLS FIRST, mes NULLS FIRST;

-- COMMAND ----------

CREATE OR REPLACE TABLE mvp_economia_guerra.gold.dim_setor (
    id_setor BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY, 
    nome_setor STRING NOT NULL, 
    CONSTRAINT pk_setor PRIMARY KEY (id_setor)
);

INSERT INTO mvp_economia_guerra.gold.dim_setor (nome_setor) 
SELECT DISTINCT setor FROM mvp_economia_guerra.silver.pib_setorial_padronizado

-- COMMAND ----------

CREATE OR REPLACE TABLE mvp_economia_guerra.gold.dim_fonte (
    id_fonte BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY, 
    nome_fonte STRING NOT NULL, 
    licenca STRING, 
    url STRING, 
    CONSTRAINT pk_fonte PRIMARY KEY (id_fonte)
    );                      
INSERT INTO mvp_economia_guerra.gold.dim_fonte (nome_fonte, licenca, url)
 VALUES
    ('World Bank WDI', 'CC BY 4.0', 'https://data360.worldbank.org/en/indicator/WB_WDI_NY_GDP_MKTP_KD_ZG'),
    ('World Bank AHLC (Sept 2024)', 'CC BY 3.0 IGO', 'https://thedocs.worldbank.org/en/doc/c25061ab26d14d7acc0330d5a7b4d496-0280012024/world-bank-economic-monitoring-report-impacts-of-the-conflict-in-the-middle-east-on-the-palestinian-economy-september-2024-update'),
    ('PCBS', 'CC BY 4.0', 'https://www.pcbs.gov.ps/'),
    ('Israel CBS', 'CBS Open License', 'https://www.cbs.gov.il/en/Pages/Enduser-license.aspx'),
    ('Israel CBS - Struggles of Business During Swords of Iron (380/2023)', 'CBS Open License', 'https://www.cbs.gov.il/he/mediarelease/doclib/2023/380/31_23_380b.pdf');

    SELECT * FROM mvp_economia_guerra.gold.dim_fonte

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Tabela Fato 1 — `fato_atividade_economica` (Perguntas 1, 2 e 3)
-- MAGIC
-- MAGIC  - `<=>` (igualdade null-safe) une o trimestre NULL do dado anual com o período anual;
-- MAGIC `p.mes IS NULL` impede que dados anuais/trimestrais se juntem com linhas mensais.

-- COMMAND ----------

CREATE OR REPLACE TABLE mvp_economia_guerra.gold.fato_atividade_economica AS 
SELECT t.id_territorio, CAST (NULL AS BIGINT) AS id_setor, p.id_periodo, f.id_fonte, 
    s.variacao_pib_pct, CAST (NULL AS DECIMAL (6,2)) AS taxa_desemprego_pct
FROM mvp_economia_guerra.silver.pib_padronizado s
JOIN mvp_economia_guerra.gold.dim_territorio t ON s.territorio = t.nome 
JOIN mvp_economia_guerra.gold.dim_periodo p ON s.ano = p.ano AND s.trimestre <=> p.trimestre AND p.mes IS NULL 
JOIN mvp_economia_guerra.gold.dim_fonte f ON s.nome_fonte = f.nome_fonte


-- COMMAND ----------

SELECT 'pib_padronizado (Silver)' AS etapa, COUNT(*) AS linhas 
FROM mvp_economia_guerra.silver.pib_padronizado

UNION ALL

SELECT 'apos JOIN territorio+periodo+fonte', COUNT(*)
FROM mvp_economia_guerra.silver.pib_padronizado s
JOIN mvp_economia_guerra.gold.dim_territorio t ON s.territorio = t.nome
JOIN mvp_economia_guerra.gold.dim_periodo p    ON s.ano = p.ano AND s.trimestre <=> p.trimestre AND p.mes IS NULL
JOIN mvp_economia_guerra.gold.dim_fonte f      ON s.nome_fonte = f.nome_fonte

UNION ALL

SELECT 'fato_atividade_economica (total)', COUNT(*)
FROM mvp_economia_guerra.gold.fato_atividade_economica;

-- COMMAND ----------

SELECT * FROM mvp_economia_guerra.gold.dim_territorio;

-- COMMAND ----------

SELECT territorio, length(territorio) AS tamanho
FROM mvp_economia_guerra.silver.pib_padronizado
WHERE territorio LIKE 'Cisjordania e Gaza%';

-- COMMAND ----------

SELECT nome, length(nome) AS tamanho
FROM mvp_economia_guerra.gold.dim_territorio
WHERE nome LIKE 'Cisjordania e Gaza%';

-- COMMAND ----------

WITH a AS (
    SELECT DISTINCT territorio AS texto FROM mvp_economia_guerra.silver.pib_padronizado
    WHERE territorio LIKE 'Cisjordania e Gaza%'
),
b AS (
    SELECT nome AS texto FROM mvp_economia_guerra.gold.dim_territorio
    WHERE nome LIKE 'Cisjordania e Gaza%'
)
SELECT a.texto AS texto_silver, b.texto AS texto_gold,
       levenshtein(a.texto, b.texto) AS diferenca_edicoes
FROM a CROSS JOIN b;

-- COMMAND ----------

UPDATE mvp_economia_guerra.gold.dim_territorio
SET nome = 'Cisjordania e Gaza (combinado)'
WHERE nome LIKE 'Cisjordania e Gaza%' OR nome LIKE 'Cisjordânia e Gaza%'

-- COMMAND ----------

SELECT nome, length(nome) FROM mvp_economia_guerra.gold.dim_territorio;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC - Antes da criação da tabela fato_atividade_economica, foi feita a correção de uma diferença de caracteres do dado "Cisjordania e Gaza (combinados)" entre as camadas Silver e Gold; 
-- MAGIC - A atualização do nome do território foi feita no mvp_economia_guerra.gold.dim_territorio, onde foi feita a remoção do caractere que estava causando o problema na comparação. 
-- MAGIC
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE mvp_economia_guerra.gold.fato_atividade_economica AS 
SELECT t.id_territorio, CAST (NULL AS BIGINT) AS id_setor, p.id_periodo, f.id_fonte, 
    s.variacao_pib_pct, CAST (NULL AS DECIMAL (6,2)) AS taxa_desemprego_pct
FROM mvp_economia_guerra.silver.pib_padronizado s
JOIN mvp_economia_guerra.gold.dim_territorio t ON s.territorio = t.nome 
JOIN mvp_economia_guerra.gold.dim_periodo p ON s.ano = p.ano AND s.trimestre <=> p.trimestre AND p.mes IS NULL 
JOIN mvp_economia_guerra.gold.dim_fonte f ON s.nome_fonte = f.nome_fonte

UNION ALL 
SELECT t.id_territorio, ds.id_setor, p.id_periodo, f.id_fonte, 
    s.variacao_pct, CAST (NULL AS DECIMAL (6,2)) AS taxa_desemprego_pct
FROM mvp_economia_guerra.silver.pib_setorial_padronizado s
JOIN mvp_economia_guerra.gold.dim_territorio t ON s.territorio = t.nome 
JOIN mvp_economia_guerra.gold.dim_setor ds ON s.setor = ds.nome_setor 
JOIN mvp_economia_guerra.gold.dim_periodo p ON s.ano = p.ano AND s.trimestre <=> p.trimestre AND p.mes IS NULL 
JOIN mvp_economia_guerra.gold.dim_fonte f ON s.nome_fonte = f.nome_fonte

UNION ALL 
SELECT t.id_territorio, CAST (NULL AS BIGINT) AS id_setor, p.id_periodo, f.id_fonte, 
    CAST (NULL AS DECIMAL (6,2)) AS variacao_pib_pct, s.taxa_desemprego_pct
FROM mvp_economia_guerra.silver.desemprego_padronizado s
JOIN mvp_economia_guerra.gold.dim_territorio t ON s.territorio = t.nome 
JOIN mvp_economia_guerra.gold.dim_periodo p ON s.ano = p.ano AND p.trimestre IS NULL AND p.mes IS NULL
JOIN mvp_economia_guerra.gold.dim_fonte f ON s.nome_fonte = f.nome_fonte;

-- COMMAND ----------

SELECT* FROM mvp_economia_guerra.gold.fato_atividade_economica

-- COMMAND ----------

-- MAGIC %md
-- MAGIC  
-- MAGIC Fato 2 — `fato_pesquisa_negocios` (Pergunta 4)
-- MAGIC
-- MAGIC - `recorte_tipo` / `recorte_nome` são dimensões degeneradas (distritos e grupos setoriais israelenses não se repetem em nenhuma outra fonte, então não justificam dimensão própria).

-- COMMAND ----------

CREATE OR REPLACE TABLE mvp_economia_guerra.gold.fato_pesquisa_negocios AS 
SELECT t.id_territorio, p.id_periodo, f.id_fonte, 
    s.recorte_tipo, s.recorte_nome, 
    s.pct_emprego_minimo, s.pct_emprego_alto, s.pct_queda_receita_severa, 
    s.pct_causa_demanda, s.pct_causa_falta_trabalhadores
FROM mvp_economia_guerra.silver.pesquisa_negocios_padronizada s
JOIN mvp_economia_guerra.gold.dim_territorio t ON s.territorio = t.nome 
JOIN mvp_economia_guerra.gold.dim_periodo p ON s.ano = p.ano AND s.mes = p.mes
JOIN mvp_economia_guerra.gold.dim_fonte f ON s.nome_fonte = f.nome_fonte

-- COMMAND ----------

SELECT* FROM mvp_economia_guerra.gold.fato_pesquisa_negocios

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Fato 3 — `fato_indicador_mensal` (Pergunta 5)
-- MAGIC
-- MAGIC - Indicadores mensais do mercado de trabalho de Israel (CBS), no grão território × mês.

-- COMMAND ----------

CREATE OR REPLACE TABLE mvp_economia_guerra.gold.fato_indicador_mensal AS 
SELECT t.id_territorio, p.id_periodo, f.id_fonte, 
    s.taxa_desemprego_sa_pct, s.ausentes_temporarios_sa_mil
FROM mvp_economia_guerra.silver.trabalho_mensal_padronizado s
JOIN mvp_economia_guerra.gold.dim_territorio t ON s.territorio = t.nome 
JOIN mvp_economia_guerra.gold.dim_periodo p ON s.ano = p.ano AND s.mes = p.mes 
JOIN mvp_economia_guerra.gold.dim_fonte f ON s.nome_fonte = f.nome_fonte

-- COMMAND ----------

SELECT * FROM mvp_economia_guerra.gold.fato_indicador_mensal


-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Chaves estrangeiras (informativas no Unity Catalog)**
-- MAGIC

-- COMMAND ----------

ALTER TABLE mvp_economia_guerra.gold.fato_atividade_economica ADD CONSTRAINT fk_ativ_territorio FOREIGN KEY (id_territorio) REFERENCES mvp_economia_guerra.gold.dim_territorio (id_territorio);
ALTER TABLE mvp_economia_guerra.gold.fato_atividade_economica ADD CONSTRAINT fk_ativ_setor     FOREIGN KEY (id_setor)      REFERENCES mvp_economia_guerra.gold.dim_setor (id_setor);
ALTER TABLE mvp_economia_guerra.gold.fato_atividade_economica ADD CONSTRAINT fk_ativ_periodo   FOREIGN KEY (id_periodo)    REFERENCES mvp_economia_guerra.gold.dim_periodo (id_periodo);
ALTER TABLE mvp_economia_guerra.gold.fato_atividade_economica ADD CONSTRAINT fk_ativ_fonte     FOREIGN KEY (id_fonte)      REFERENCES mvp_economia_guerra.gold.dim_fonte (id_fonte);
ALTER TABLE mvp_economia_guerra.gold.fato_pesquisa_negocios   ADD CONSTRAINT fk_neg_periodo    FOREIGN KEY (id_periodo)    REFERENCES mvp_economia_guerra.gold.dim_periodo (id_periodo);
ALTER TABLE mvp_economia_guerra.gold.fato_pesquisa_negocios   ADD CONSTRAINT fk_neg_fonte      FOREIGN KEY (id_fonte)      REFERENCES mvp_economia_guerra.gold.dim_fonte (id_fonte);
ALTER TABLE mvp_economia_guerra.gold.fato_indicador_mensal    ADD CONSTRAINT fk_men_territorio FOREIGN KEY (id_territorio) REFERENCES mvp_economia_guerra.gold.dim_territorio (id_territorio);
ALTER TABLE mvp_economia_guerra.gold.fato_indicador_mensal    ADD CONSTRAINT fk_men_periodo    FOREIGN KEY (id_periodo)    REFERENCES mvp_economia_guerra.gold.dim_periodo (id_periodo);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Catálogo de dados** — comentários das tabelas e colunas Gold
-- MAGIC
-- MAGIC -  Descrição de cada tabela e coluna registrada no Unity Catalog

-- COMMAND ----------

COMMENT ON TABLE mvp_economia_guerra.gold.dim_territorio IS 'Dimensão: territórios analisados, incluindo territórios agregados por algumas das fontes.';
ALTER TABLE mvp_economia_guerra.gold.dim_territorio ALTER COLUMN id_territorio COMMENT 'Chave substituta (PK)';
ALTER TABLE mvp_economia_guerra.gold.dim_territorio ALTER COLUMN nome COMMENT 'Nome padronizado do território'; 
ALTER TABLE mvp_economia_guerra.gold.dim_territorio ALTER COLUMN regiao COMMENT 'Região geográfica do território';

-- COMMAND ----------

COMMENT ON TABLE mvp_economia_guerra.gold.dim_periodo IS 'Dimensão: tempo com granularidade mista; anual, trimestral e mensal.'; 
ALTER TABLE mvp_economia_guerra.gold.dim_periodo ALTER COLUMN id_periodo COMMENT 'Chave substituta (PK)'; 
ALTER TABLE mvp_economia_guerra.gold.dim_periodo ALTER COLUMN ano COMMENT 'Ano'; 
ALTER TABLE mvp_economia_guerra.gold.dim_periodo ALTER COLUMN trimestre COMMENT 'Trimestre (1-4); NULL em linhas anuais'; 
ALTER TABLE mvp_economia_guerra.gold.dim_periodo ALTER COLUMN mes COMMENT 'Mês (1-12); NULL em linhas trimestrais e anuais';

-- COMMAND ----------

COMMENT ON TABLE mvp_economia_guerra.gold.dim_setor IS 'Dimensão: setores econômicos do relatório do World Bank (set/2024).';
ALTER TABLE mvp_economia_guerra.gold.dim_setor ALTER COLUMN id_setor COMMENT 'Chave substituta (PK)'; 
ALTER TABLE mvp_economia_guerra.gold.dim_setor ALTER COLUMN nome_setor COMMENT 'Nome do setor econômico';


-- COMMAND ----------

COMMENT ON TABLE mvp_economia_guerra.gold.dim_fonte IS 'Dinmensão: fontes e licenças dos dados usados.';
ALTER TABLE mvp_economia_guerra.gold.dim_fonte ALTER COLUMN id_fonte COMMENT 'Chave substituta (PK)';
ALTER TABLE mvp_economia_guerra.gold.dim_fonte ALTER COLUMN nome_fonte COMMENT 'Nome da fonte';
ALTER TABLE mvp_economia_guerra.gold.dim_fonte ALTER COLUMN licenca COMMENT 'Licença de uso';
ALTER TABLE mvp_economia_guerra.gold.dim_fonte ALTER COLUMN url COMMENT 'Link para a fonte original';

-- COMMAND ----------

COMMENT ON TABLE mvp_economia_guerra.gold.fato_atividade_economica IS 'Fato: variação do PIB (total e setorial) e taxa de desemprego anual por território, período, setor e fonte. Perguntas 1 a 3.';
ALTER TABLE mvp_economia_guerra.gold.fato_atividade_economica ALTER COLUMN id_territorio COMMENT 'FK para dim_territorio';
ALTER TABLE mvp_economia_guerra.gold.fato_atividade_economica ALTER COLUMN id_setor COMMENT 'FK para dim_setor; NULL quando a linha é PIB total ou desemprego';
ALTER TABLE mvp_economia_guerra.gold.fato_atividade_economica ALTER COLUMN id_periodo COMMENT 'FK para dim_periodo (anual ou trimestral)';
ALTER TABLE mvp_economia_guerra.gold.fato_atividade_economica ALTER COLUMN id_fonte COMMENT 'FK para dim_fonte';
ALTER TABLE mvp_economia_guerra.gold.fato_atividade_economica ALTER COLUMN variacao_pib_pct COMMENT 'Variação percentual do PIB (anual ou trimestre) comparada contra o mesmo trimestre do ano anterior pode ser negativa';
ALTER TABLE mvp_economia_guerra.gold.fato_atividade_economica ALTER COLUMN taxa_desemprego_pct COMMENT 'Taxa de desemprego anual em percentual (padrão ILO/ICLS-19th); NULL em Gaza a partir de 2023';

-- COMMAND ----------

COMMENT ON TABLE mvp_economia_guerra.gold.fato_pesquisa_negocios IS 'Fato: pesquisa emergencial do governo de Israel (Israel Bureau of Statistic) (380/2023) sobre negócios na guerra, por onda e recorte. Pergunta 4.';
ALTER TABLE mvp_economia_guerra.gold.fato_pesquisa_negocios ALTER COLUMN id_territorio COMMENT 'FK para dim_territorio (Israel)';
ALTER TABLE mvp_economia_guerra.gold.fato_pesquisa_negocios ALTER COLUMN id_periodo COMMENT 'FK para dim_periodo (mês da onda)';
ALTER TABLE mvp_economia_guerra.gold.fato_pesquisa_negocios ALTER COLUMN id_fonte COMMENT 'FK para dim_fonte';
ALTER TABLE mvp_economia_guerra.gold.fato_pesquisa_negocios ALTER COLUMN recorte_tipo COMMENT 'Dimensão degenerada: nacional, distrito, setor ou porte';
ALTER TABLE mvp_economia_guerra.gold.fato_pesquisa_negocios ALTER COLUMN recorte_nome COMMENT 'Dimensão degenerada: valor do recorte';
ALTER TABLE mvp_economia_guerra.gold.fato_pesquisa_negocios ALTER COLUMN pct_emprego_minimo COMMENT 'percentual de negócios com até 20% da equipe pré gurra ativa';
ALTER TABLE mvp_economia_guerra.gold.fato_pesquisa_negocios ALTER COLUMN pct_emprego_alto COMMENT ' percentual de negócios com 81% ou mais da equipe ativa';
ALTER TABLE mvp_economia_guerra.gold.fato_pesquisa_negocios ALTER COLUMN pct_queda_receita_severa COMMENT 'percentual de negócios com queda de receita acima de 50%';
ALTER TABLE mvp_economia_guerra.gold.fato_pesquisa_negocios ALTER COLUMN pct_causa_demanda COMMENT 'percentual que aponta queda de demanda como causa principal da diminuição da receita';
ALTER TABLE mvp_economia_guerra.gold.fato_pesquisa_negocios ALTER COLUMN pct_causa_falta_trabalhadores COMMENT '% que aponta falta de trabalhadores como causa principal da diminuição da receita';
 

-- COMMAND ----------

COMMENT ON TABLE mvp_economia_guerra.gold.fato_indicador_mensal IS 'Fato: indicadores mensais do mercado de trabalho de Israel (dessazonalizados). Pergunta 5.';
ALTER TABLE mvp_economia_guerra.gold.fato_indicador_mensal ALTER COLUMN id_territorio COMMENT 'FK para dim_territorio';
ALTER TABLE mvp_economia_guerra.gold.fato_indicador_mensal ALTER COLUMN id_periodo COMMENT 'FK para dim_periodo (mensal)';
ALTER TABLE mvp_economia_guerra.gold.fato_indicador_mensal ALTER COLUMN id_fonte COMMENT 'FK para dim_fonte';
ALTER TABLE mvp_economia_guerra.gold.fato_indicador_mensal ALTER COLUMN taxa_desemprego_sa_pct COMMENT 'Taxa de desemprego dessazonalizada em percentual';
ALTER TABLE mvp_economia_guerra.gold.fato_indicador_mensal ALTER COLUMN ausentes_temporarios_sa_mil COMMENT 'Trabalhadores temporariamente ausentes, milhares, dessazonalizado';

-- COMMAND ----------

SHOW TABLES IN mvp_economia_guerra.gold