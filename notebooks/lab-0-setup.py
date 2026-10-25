# Databricks notebook source
# MAGIC %md
# MAGIC # Mini-curso · Setup — 1 GB de trades de BTC
# MAGIC
# MAGIC Rode este notebook **uma vez**, antes dos labs. Ele gera 20 milhões de trades sintéticos de BTC/USD
# MAGIC (o ano de 2025, um trade a cada ~1,6 s) e grava num Volume do Unity Catalog como **8 arquivos CSV de
# MAGIC ~125 MB**, ~1 GB no total. Nada é baixado da internet.
# MAGIC
# MAGIC O dado é **equilibrado de propósito**: os 8 arquivos têm o mesmo tamanho, e metade dos trades é `buy`,
# MAGIC metade `sell`. Na leitura, cada arquivo vira exatamente uma partição (o limite padrão é 128 MB por
# MAGIC partição). Assim, os números que você vê batem com os do vídeo.
# MAGIC
# MAGIC **Compute:** clássico all-purpose, Databricks Runtime **17.3 LTS** (Spark 4.0), access mode **Dedicated**,
# MAGIC 1 worker com 4 cores (no vídeo: `Standard_D4ds_v5`, no Azure), **Photon desligado**.

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

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{SCHEMA}.{VOLUME}")

N = 20_000_000                 # trades
INICIO = 1735689600            # 2025-01-01 00:00:00 UTC
UM_ANO = 365 * 24 * 3600       # segundos

trades = (
    spark.range(0, N, 1, numPartitions=8)          # 8 partições → 8 arquivos
    .select(
        (F.col("id") + 100_000_000).alias("trade_id"),          # sempre 9 dígitos
        F.date_format(
            F.timestamp_seconds(F.lit(INICIO) + (F.col("id") * UM_ANO / N).cast("long")),
            "yyyy-MM-dd HH:mm:ss",
        ).alias("ts"),
        (F.lit(60000) + F.lit(15000) * F.sin(F.col("id") / 1_000_000) + (F.col("id") % 997) / 100)
            .cast("decimal(10,2)").alias("price"),               # entre 45 mil e 75 mil
        (((F.col("id") * 7919) % 999_000) / 1_000_000 + 0.001)
            .cast("decimal(8,6)").alias("amount"),               # 0,001 a 1 BTC
        F.when(F.col("id") % 2 == 0, "buy").otherwise("sell").alias("side"),
    )
)

trades.write.mode("overwrite").option("header", True).csv(PASTA)

# COMMAND ----------

import os

arquivos = sorted(f for f in os.listdir(PASTA) if f.startswith("part-"))
tamanhos = [os.path.getsize(os.path.join(PASTA, f)) / 2**20 for f in arquivos]
for f, t in zip(arquivos, tamanhos):
    print(f"{f[:10]}…  {t:,.2f} MiB")
print(f"{len(arquivos)} arquivos · {sum(tamanhos):,.0f} MiB no total")
