from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans

spark = SparkSession.builder \
    .appName("Game Clustering") \
    .getOrCreate()

jdbc_url = "jdbc:postgresql://localhost:5432/datasteam"
db_properties = {
    "user": "letranhoailoc",
    "password": "letranhoailoc9",
    "driver": "org.postgresql.Driver"
}

games = spark.read.jdbc(url=jdbc_url, table="games", properties=db_properties)
game_details = spark.read.jdbc(url=jdbc_url, table="game_details", properties=db_properties)

df = games.join(game_details, "game_id") \
    .select(
        "game_id",
        "name",
        "game_tag",
        col("final_price_number_original").alias("price_original"),
        col("final_price_number_discount").alias("price_discount"),
        "review_summary"
    ) \
    .filter(col("price_original").isNotNull())

df = df.withColumn(
    "discount_percent",
    when(col("price_discount").isNotNull(),
         (col("price_original") - col("price_discount")) / col("price_original"))
    .otherwise(0.0)
).withColumn(
    "discount_percent",
    when(col("discount_percent") < 0, 0.0)
    .when(col("discount_percent") > 1.0, 1.0)
    .otherwise(col("discount_percent"))
)

assembler = VectorAssembler(
    inputCols=["price_original", "discount_percent"],
    outputCol="features",
    handleInvalid="skip"
)

df_vec = assembler.transform(df)

scaler = StandardScaler(
    inputCol="features",
    outputCol="scaled_features",
    withMean=True,
    withStd=True
)

df_scaled = scaler.fit(df_vec).transform(df_vec)

kmeans = KMeans(k=4, seed=42, featuresCol="scaled_features", predictionCol="cluster")
model = kmeans.fit(df_scaled)
result = model.transform(df_scaled)

final_result = result.select(
    "game_id", "name", "game_tag", "price_original",
    "price_discount", "discount_percent", "cluster", "review_summary"
)

print("Cluster Centers (scaled):")
for i, center in enumerate(model.clusterCenters()):
    print(f"Cluster {i}: {center}")

print("\nGames per cluster:")
final_result.groupBy("cluster").count().orderBy("cluster").show()

final_result.write \
    .mode("overwrite") \
    .jdbc(url=jdbc_url, table="ml_game_clusters", properties=db_properties)

print("✅ CLUSTERING DONE - Results saved to ml_game_clusters")

spark.stop()

# spark-submit --packages org.postgresql:postgresql:42.7.4 game_clustering.py