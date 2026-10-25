# Databricks notebook source
# MAGIC %md
# MAGIC # Mini-curso · Job, stage e task
# MAGIC
# MAGIC Neste lab você vai contar jobs, stages e tasks de três consultas. Depois de rodar cada célula, abra o
# MAGIC **Spark Jobs** que aparece embaixo da saída.
# MAGIC
# MAGIC **Antes de começar**
# MAGIC - Use um compute clássico all-purpose com Databricks Runtime **17.3 LTS** (Spark 4.0), access mode
# MAGIC   **Dedicated**, 1 worker com 4 cores (no vídeo: `Standard_D4ds_v5`, no Azure) e **Photon desligado**.
# MAGIC   Com outro número de cores, alguns números saem diferentes dos do vídeo.
# MAGIC - Rode o notebook `lab-0-setup` uma vez: ele gera o dataset que todos os labs usam.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração
# MAGIC
# MAGIC Todos os labs usam o mesmo catálogo, schema e Volume. `CATALOGO` pega o catálogo padrão do seu
# MAGIC workspace; troque pelo nome de outro catálogo se quiser. Você precisa de permissão para criar schema nele.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOGO = spark.catalog.currentCatalog()
SCHEMA = "mini_curso"
VOLUME = "dados"

assert CATALOGO not in ("hive_metastore", "spark_catalog"), (
    "CATALOGO precisa ser um catálogo do Unity Catalog — troque o valor acima pelo nome do seu."
)
PASTA = f"/Volumes/{CATALOGO}/{SCHEMA}/{VOLUME}/trades"
ESQUEMA = "trade_id LONG, ts TIMESTAMP, price DECIMAL(10,2), amount DECIMAL(8,6), side STRING"
print(PASTA)
# COMMAND ----------

df = spark.read.csv(PASTA, header=True, schema=ESQUEMA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## A · Só transformação: nenhum job

# COMMAND ----------

compras_caras = df.filter("side = 'buy' AND price > 70000").select("trade_id", "price")

# COMMAND ----------

# MAGIC %md
# MAGIC ## B · Uma action sem shuffle: 1 job, 1 stage, 8 tasks

# COMMAND ----------

df.filter("price > 99999").collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## C · Uma action com shuffle (`groupBy`): 2 stages
# MAGIC
# MAGIC Com o AQE ligado (padrão), aparecem **2 jobs**: o primeiro roda a stage antes do shuffle (8 tasks);
# MAGIC o segundo mostra essa stage como **skipped** e roda a stage depois do shuffle (1 task).

# COMMAND ----------

df.groupBy("side").agg(F.sum("amount").alias("btc_negociado")).collect()
