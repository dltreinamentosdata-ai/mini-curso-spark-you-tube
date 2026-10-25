# Databricks notebook source
# MAGIC %md
# MAGIC # Mini-curso · O cluster por dentro: worker, executor, cluster manager
# MAGIC
# MAGIC Antes de rodar: abra a página do **Compute** e mostre o tipo do driver e do worker.
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
# MAGIC ## 1 · Quantas tasks o cluster roda ao mesmo tempo?
# MAGIC
# MAGIC `defaultParallelism` é o total de cores dos executors. No cluster do curso: 1 worker × 4 cores.

# COMMAND ----------

spark.sparkContext.defaultParallelism

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Quem executa as tasks?
# MAGIC
# MAGIC Uma consulta que lê o 1 GB inteiro (nenhum trade passa de 99.999, então volta vazio).
# MAGIC Depois abra **Spark UI › Executors**: o `driver` fica com 0 tasks; todas as tasks rodaram no executor.
# MAGIC (A aba soma desde que o cluster subiu. Pra ver exatamente 8, reinicie o cluster antes desta célula.)
# COMMAND ----------

df = spark.read.csv(PASTA, header=True, schema=ESQUEMA)

# COMMAND ----------

df.filter("price > 99999").collect()
