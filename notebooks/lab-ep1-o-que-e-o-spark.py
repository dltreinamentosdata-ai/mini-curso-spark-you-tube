# Databricks notebook source
# MAGIC %md
# MAGIC # Mini-curso · O que é o Spark (e o que ele não é)
# MAGIC
# MAGIC O dado mora no **storage** (aqui, um Volume). O Spark **lê, processa e devolve** o resultado.
# MAGIC Ele não guarda nada: desligue o cluster e os arquivos continuam lá.
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

# MAGIC %md
# MAGIC ## 1 · O dado está no storage
# MAGIC
# MAGIC 8 arquivos CSV, ~125 MB cada, ~1 GB no total. Nenhum deles está "dentro" do Spark.

# COMMAND ----------

import os

arquivos = sorted(f for f in os.listdir(PASTA) if f.startswith("part-"))
tamanhos = [os.path.getsize(os.path.join(PASTA, f)) / 2**20 for f in arquivos]
for f, t in zip(arquivos, tamanhos):
    print(f"{f[:10]}…  {t:,.2f} MiB")
print(f"{len(arquivos)} arquivos · {sum(tamanhos):,.0f} MiB no total")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · O Spark lê e processa
# MAGIC
# MAGIC Quanto BTC foi negociado em cada lado: compra (`buy`) e venda (`sell`)?
# COMMAND ----------

df = spark.read.csv(PASTA, header=True, schema=ESQUEMA)

# COMMAND ----------

volume_por_lado = df.groupBy("side").agg(F.sum("amount").alias("btc_negociado")).collect()
volume_por_lado

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · O que sobra quando o cluster desliga
# MAGIC
# MAGIC - Os **8 arquivos** continuam no Volume: são do storage.
# MAGIC - O **resultado** (`volume_por_lado`) era só uma variável na memória do driver: some com o cluster.
# MAGIC   Pra guardar, você grava de volta no storage (tabela ou arquivo).
