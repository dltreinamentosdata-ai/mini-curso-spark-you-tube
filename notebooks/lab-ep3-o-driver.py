# Databricks notebook source
# MAGIC %md
# MAGIC # Mini-curso · O driver: o cérebro da aplicação Spark
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
# MAGIC ## 1 · O plano antes de rodar
# MAGIC
# MAGIC `explain()` mostra o plano que o driver montou. Confira em **Spark UI › Jobs**: nenhum job novo.

# COMMAND ----------

df.filter("side = 'buy'").select("price", "amount").explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · O resultado volta pro driver
# MAGIC
# MAGIC As tasks somam nos executors; o driver recebe só as 2 linhas do resultado.
# MAGIC Enquanto roda, a barra de progresso é o driver acompanhando as tasks.

# COMMAND ----------

resultado = df.groupBy("side").agg(F.sum("amount").alias("btc_negociado")).collect()
print(type(resultado), len(resultado))
resultado

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · O collect() que derruba o driver
# MAGIC
# MAGIC O limite de resultado que o driver aceita (no Apache Spark, o padrão é 1g; confira o do seu cluster):

# COMMAND ----------

spark.sparkContext.getConf().get("spark.driver.maxResultSize", "não definido: vale o padrão de 1g")

# COMMAND ----------

# MAGIC %md
# MAGIC **Não rode.** Isto traria os 20 milhões de linhas pra memória do driver, que não foi feita pra
# MAGIC guardar o dataset inteiro.
# MAGIC
# MAGIC ```python
# MAGIC df.collect()
# MAGIC ```
