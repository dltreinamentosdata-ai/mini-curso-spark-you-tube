# Databricks notebook source
# MAGIC %md
# MAGIC # Mini-curso · O cluster por dentro: worker, executor, cluster manager
# MAGIC
# MAGIC Neste lab você vai identificar as peças do seu cluster e ver onde as tasks rodam.
# MAGIC
# MAGIC Antes de rodar, abra a página do seu **Compute** e confira o tipo de máquina do driver e do worker.
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

# MAGIC %md
# MAGIC ## 1 · Quantas tasks o cluster roda ao mesmo tempo?
# MAGIC
# MAGIC `defaultParallelism` é o total de cores dos executors: o máximo de tasks que rodam ao mesmo tempo.
# MAGIC Com 1 worker de 4 cores, o resultado é 4.

# COMMAND ----------

spark.sparkContext.defaultParallelism

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Quem executa as tasks?
# MAGIC
# MAGIC A consulta abaixo lê o 1 GB inteiro (nenhum trade passa de 99.999, então o resultado volta vazio).
# MAGIC Depois abra **Spark UI › Executors** e compare as linhas: o `driver` fica com 0 tasks, e todas as tasks
# MAGIC rodaram no executor. A aba soma tudo desde que o cluster ligou; para ver exatamente 8, reinicie o
# MAGIC cluster antes desta célula.

# COMMAND ----------

df = spark.read.csv(PASTA, header=True, schema=ESQUEMA)

# COMMAND ----------

df.filter("price > 99999").collect()
