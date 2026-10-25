# Databricks notebook source
# MAGIC %md
# MAGIC # Mini-curso · Partições, tasks e cores
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
# MAGIC ## 1 · Quantas partições?
# MAGIC
# MAGIC 8 arquivos de ~125 MB, e cada partição vai até 128 MB por padrão: 8 partições.
# MAGIC (Em cluster com mais de 8 cores o Spark divide mais, pra ocupar os cores. No cluster do curso, 8.)

# COMMAND ----------

print("partições:", df.rdd.getNumPartitions())
print("cores:", spark.sparkContext.defaultParallelism)
print("limite por partição:", spark.conf.get("spark.sql.files.maxPartitionBytes"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · 8 tasks em 4 cores: duas ondas
# MAGIC
# MAGIC Depois abra **Spark UI › Jobs › o job › a stage**:
# MAGIC - **Event Timeline**: 4 barras juntas, depois mais 4;
# MAGIC - tabela de tasks, coluna **Launch Time**: 4 começam juntas, as outras 4 quando as primeiras terminam.

# COMMAND ----------

df.filter("price > 99999").collect()
