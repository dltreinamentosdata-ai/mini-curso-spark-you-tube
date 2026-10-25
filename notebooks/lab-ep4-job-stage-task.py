# Databricks notebook source
# MAGIC %md
# MAGIC # Mini-curso · Job, stage e task
# MAGIC
# MAGIC Em cada célula, abra o **Spark Jobs** que aparece embaixo da saída e conte jobs, stages e tasks.
# MAGIC
# MAGIC **Ambiente:** compute clássico all-purpose, DBR 17.3 LTS (Spark 4.0), access mode **Dedicated**,
# MAGIC 1 driver + 1 worker `Standard_D4ds_v5` (4 cores), **Photon desligado**. Rode antes o
# MAGIC `lab-0-setup`, que gera o dataset.
# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração
# MAGIC
# MAGIC Mesmo catálogo, schema e Volume em todos os labs do mini-curso. `CATALOGO` pega o catálogo padrão
# MAGIC do workspace; troque se quiser outro.

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
