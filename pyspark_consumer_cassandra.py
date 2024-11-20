from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, from_json, sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType

# Inicializa o SparkSession
spark = SparkSession.builder \
    .appName("Aggregate Product Sales from Kafka") \
    .config("spark.cassandra.connection.host", "127.0.0.1") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

# Define o esquema dos produtos (caso a coluna 'products' esteja em formato de string)
product_schema = ArrayType(
    StructType([
        StructField("product_name", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True)
    ])
)

# Lê os dados do Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ecommerce_sales") \
    .option("startingOffsets", "latest") \
    .load()

# Define o esquema do conteúdo da mensagem (JSON com os dados de venda)
schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("client_document", StringType(), True),
    StructField("products", product_schema, True),
    StructField("total_value", DoubleType(), True),
    StructField("sale_datetime", StringType(), True)
])

# Processa as mensagens do Kafka
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select(
        col("data.order_id"),
        col("data.client_document"),
        col("data.products"),
        col("data.total_value"),
        col("data.sale_datetime")
    )

# Explode a coluna "products" para obter cada produto separadamente
products_df = parsed_df.select(explode(col("products")).alias("product")) \
    .select(
        col("product.product_name").alias("product_name"),
        (col("product.quantity") * col("product.price")).alias("sale_value")
    )

# Agrega as vendas por produto
aggregated_sales_df = products_df.groupBy("product_name") \
    .agg(_sum("sale_value").alias("total_sales"))

# Escreve o resultado na tabela "product_sales" no Cassandra (com upsert)
query = aggregated_sales_df \
    .writeStream \
    .outputMode("update") \
    .foreachBatch(lambda batch_df, batch_id: (
        batch_df \
        .write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="product_sales", keyspace="ecommerce") \
        .save()
    )) \
    .start()

query.awaitTermination()
