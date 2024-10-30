from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, DoubleType

# Lista de produtos predefinidos
predefined_products = [
    "Laptop", "Smartphone", "Tablet", "Headphones", "Monitor",
    "Keyboard", "Mouse", "Printer", "Router", "Webcam",
    "Microphone", "Speakers", "Graphics Card", "Motherboard",
    "RAM", "SSD", "Hard Drive", "Power Supply", "Cooling Fan",
    "Smartwatch"
]

# Crie uma sessão Spark e adicione o conector Kafka
spark = SparkSession.builder \
    .appName("KafkaEcommerceConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
    .getOrCreate()

# Configuração para leitura do Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ecommerce_sales") \
    .option("startingOffsets", "latest") \
    .load()

# Converte o valor da mensagem de bytes para string JSON
sales_df = df.selectExpr("CAST(value AS STRING)")

# Define o esquema dos dados
schema = StructType([
    StructField("order_id", StringType()),
    StructField("client_document", StringType()),
    StructField("products", ArrayType(StructType([
        StructField("product_name", StringType()),
        StructField("quantity", IntegerType()),
        StructField("price", DoubleType())
    ]))),
    StructField("total_value", DoubleType()),
    StructField("sale_datetime", StringType())
])

# Realiza o parsing do JSON com o schema definido
parsed_df = sales_df.selectExpr(f"from_json(value, '{schema.simpleString()}') as data").select("data.*")

# Explode a lista de produtos para transformar cada produto em uma linha individual
exploded_df = parsed_df.select("order_id", "sale_datetime", explode("products").alias("product"))
exploded_df = exploded_df.select("order_id", "sale_datetime", 
                                  col("product.product_name").alias("product_name"), 
                                  col("product.quantity").alias("quantity"), 
                                  col("product.price").alias("price"))

# Filtra os produtos com base na lista predefinida
filtered_df = exploded_df.filter(col("product_name").isin(predefined_products))

# Calcula o valor total por produto e agrupa
aggregated_df = filtered_df.groupBy("product_name").agg(
    spark_sum(col("quantity") * col("price")).alias("total_sales")
)

# Ordena os resultados pelo total de vendas em ordem decrescente
sorted_df = aggregated_df.orderBy(col("total_sales").desc())

# Exibe o resultado na tela
query = sorted_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("Consumer stopped.")
except Exception as e:
    print(f"An error occurred: {e}")
