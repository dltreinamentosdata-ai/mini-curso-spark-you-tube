# Databricks notebook source
# MAGIC %md
# MAGIC # Mini-curso · Partições, tasks e cores
# MAGIC
# MAGIC Neste lab você vai ver quantas partições o Spark cria ao ler 1 GB e como 8 tasks rodam em 4 cores.
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
# MAGIC Todos os labs usam o mesmo catálogo, schema e volume: `<catálogo>.mini_curso.landing`. `CATALOGO` pega o
# MAGIC catálogo padrão do seu workspace; troque pelo nome de outro catálogo se quiser. Você precisa de permissão
# MAGIC para criar schema nele.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOGO = spark.catalog.currentCatalog()
SCHEMA = "mini_curso"
VOLUME = "landing"

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
# MAGIC São 8 arquivos de ~125 MB, e cada partição vai até 128 MB por padrão: 8 partições.
# MAGIC Em um cluster com mais de 8 cores, o Spark divide em mais partições para ocupar os cores;
# MAGIC com 4 cores, como no vídeo, são 8. O limite aparece em bytes: 134217728 bytes = 128 MB.

# COMMAND ----------

print("partições:", df.rdd.getNumPartitions())
print("cores:", spark.sparkContext.defaultParallelism)
print("limite por partição:", spark.conf.get("spark.sql.files.maxPartitionBytes"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · 8 tasks em 4 cores: duas ondas
# MAGIC
# MAGIC Rode a célula e abra **Spark UI › Jobs › o job › a stage**:
# MAGIC - **Event Timeline**: 4 barras juntas, depois mais 4;
# MAGIC - tabela de tasks, coluna **Launch Time**: 4 começam juntas, as outras 4 quando as primeiras terminam.

# COMMAND ----------

df.filter("price > 99999").collect()
