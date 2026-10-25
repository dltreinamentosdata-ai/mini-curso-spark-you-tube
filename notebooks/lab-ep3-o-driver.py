# Databricks notebook source
# MAGIC %md
# MAGIC # Mini-curso · O driver: o cérebro da aplicação Spark
# MAGIC
# MAGIC Neste lab você vai ver o driver montando o plano de execução sem rodar nada, recebendo o resultado
# MAGIC de uma consulta e o limite que protege a memória dele.
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
# MAGIC ## 1 · O plano antes de rodar
# MAGIC
# MAGIC `explain()` mostra o plano físico que o driver montou. Depois confira em **Spark UI › Jobs**:
# MAGIC nenhum job novo apareceu, porque nada foi executado.

# COMMAND ----------

df.filter("side = 'buy'").select("price", "amount").explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · O resultado volta pro driver
# MAGIC
# MAGIC As tasks somam nos executors, e o driver recebe só as 2 linhas do resultado.
# MAGIC Enquanto a célula roda, observe a barra de progresso: é o driver acompanhando as tasks.

# COMMAND ----------

resultado = df.groupBy("side").agg(F.sum("amount").alias("btc_negociado")).collect()
print(type(resultado), len(resultado))
resultado

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · O collect() que derruba o driver
# MAGIC
# MAGIC Veja o limite de resultado que o driver aceita no seu cluster (no Apache Spark, o padrão é 1g):

# COMMAND ----------

spark.sparkContext.getConf().get("spark.driver.maxResultSize", "não definido: vale o padrão de 1g")

# COMMAND ----------

# MAGIC %md
# MAGIC **Não rode o comando abaixo.** Ele traria os 20 milhões de linhas para a memória do driver, que não foi
# MAGIC feita para guardar o dataset inteiro: o Spark aborta o job pelo limite acima ou o driver fica sem memória.
# MAGIC
# MAGIC ```python
# MAGIC df.collect()
# MAGIC ```
