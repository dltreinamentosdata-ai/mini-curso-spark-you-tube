# Databricks notebook source
# MAGIC %md
# MAGIC # Mini-curso · O que é o Spark (e o que ele não é)
# MAGIC
# MAGIC Neste lab você vai ver, na prática, que o Spark **processa** dados mas não os **guarda**: os arquivos
# MAGIC ficam no storage (aqui, um Volume do Unity Catalog), e o Spark lê, processa e devolve o resultado.
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

# MAGIC %md
# MAGIC ## 1 · O dado está no storage
# MAGIC
# MAGIC Liste os arquivos do Volume. São 8 CSVs de ~125 MB, ~1 GB no total, e nenhum deles está "dentro" do Spark.

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
# MAGIC   Para guardar um resultado, você grava de volta no storage (tabela ou arquivo).
# MAGIC
# MAGIC Teste: termine o cluster, ligue de novo e liste o Volume outra vez.
