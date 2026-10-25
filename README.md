# Como o Spark funciona por dentro — labs do mini-curso

Notebooks Databricks do mini-curso gratuito **Como o Spark funciona por dentro** (DL Treinamentos), publicado no YouTube em [@DouglasGLittig](https://www.youtube.com/@DouglasGLittig).

## Conteúdo

| Notebook | Episódio |
|---|---|
| `notebooks/lab-0-setup.py` | Setup: gera o dataset (rode uma vez, antes dos outros) |
| `notebooks/lab-ep1-o-que-e-o-spark.py` | O que é o Apache Spark |
| `notebooks/lab-ep2-o-cluster.py` | Arquitetura do cluster: driver, worker, executor, cluster manager |
| `notebooks/lab-ep3-o-driver.py` | O driver: plano de execução e `collect()` |
| `notebooks/lab-ep4-job-stage-task.py` | Job, stage e task |
| `notebooks/lab-ep5-particoes.py` | Partições, tasks e slots |

## Como usar

1. No workspace Databricks, crie uma **Git folder** apontando para este repositório. Se preferir, use **Workspace → Import → URL** com o link de cada notebook.
2. Rode no compute usado nos vídeos: Databricks Runtime **17.3 LTS** (Spark 4.0), compute clássico all-purpose, access mode **Dedicated**, **1 worker** de 4 cores (`Standard_D4ds_v5` no Azure), Unity Catalog, **Photon desligado**. Com outro número de cores, o número de partições e de ondas muda.
3. Rode o `lab-0-setup` uma vez. Depois, os labs na ordem dos episódios.

## Dataset

20 milhões de trades **sintéticos** de BTC/USD, um a cada ~1,6 s ao longo de 2025, gerados pelo próprio `lab-0-setup`. Nenhum arquivo de dados fica neste repositório e nada é baixado da internet.

O setup grava 8 arquivos CSV de ~125 MB (~1 GB no total) no volume `landing` do Unity Catalog, em `/Volumes/<catálogo>/mini_curso/landing/trades/`. O dado é equilibrado de propósito: os 8 arquivos têm o mesmo tamanho e metade dos trades é compra, metade venda. Na leitura, cada arquivo vira uma partição (o limite padrão é 128 MB por partição), e os números que você vê batem com os do vídeo.

| Coluna | Tipo | Exemplo |
|---|---|---|
| `trade_id` | `LONG` | `100000000` |
| `ts` | `TIMESTAMP` | `2025-01-01 00:00:00` |
| `price` | `DECIMAL(10,2)` | `60000.00` |
| `amount` | `DECIMAL(8,6)` | `0.001000` |
| `side` | `STRING` | `buy` / `sell` |

## Storage externo (opcional)

No vídeo, o volume `landing` é **externo**: aponta para o container `landing` de uma conta ADLS Gen2 no Azure, e os arquivos aparecem no portal. Se você pular esta parte, o `lab-0-setup` cria um volume gerenciado com o mesmo nome e os labs funcionam igual.

Para montar como no vídeo (precisa de permissão de Owner no resource group e de admin do metastore):

1. **Conta de storage:** crie uma Storage account na mesma região do workspace, com **Advanced → Enable hierarchical namespace** marcado. Sem isso, o Databricks não consegue usar a conta. Dentro dela, crie o container `landing` e o diretório `mini-curso`.
2. **Access Connector for Azure Databricks:** crie no mesmo resource group, com a managed identity ligada, e copie o **Resource ID**.
3. **Permissão:** na conta de storage, **Access Control (IAM) → Add role assignment → Storage Blob Data Contributor** para o Access Connector.
4. **Storage credential:** no Databricks, **Catalog → + → Create a credential**, tipo **Azure Managed Identity**, com o Resource ID do passo 2.
5. **External location e volume:**

```sql
CREATE EXTERNAL LOCATION loc_landing
URL 'abfss://landing@<sua-conta>.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL <sua-credencial>);

USE CATALOG <seu-catálogo>;
CREATE SCHEMA IF NOT EXISTS mini_curso;
CREATE EXTERNAL VOLUME mini_curso.landing
LOCATION 'abfss://landing@<sua-conta>.dfs.core.windows.net/mini-curso';
```

Passo a passo completo na documentação:
- [Criar uma conta para Data Lake Storage](https://learn.microsoft.com/en-us/azure/storage/blobs/create-data-lake-storage-account)
- [Conectar a uma external location ADLS Gen2](https://learn.microsoft.com/en-us/azure/databricks/connect/unity-catalog/cloud-storage/external-locations-adls)
- [Criar volumes](https://learn.microsoft.com/en-us/azure/databricks/volumes/utility-commands)

## Documentação oficial

- [Apache Spark 4.0 — Overview](https://spark.apache.org/docs/4.0.0/index.html)
- [Apache Spark 4.0 — Cluster Mode Overview](https://spark.apache.org/docs/4.0.0/cluster-overview.html)
- [Apache Spark 4.0 — RDD Programming Guide](https://spark.apache.org/docs/4.0.0/rdd-programming-guide.html)
- [Apache Spark 4.0 — Performance Tuning](https://spark.apache.org/docs/4.0.0/sql-performance-tuning.html)
- [Apache Spark 4.0 — Configuration](https://spark.apache.org/docs/4.0.0/configuration.html)
