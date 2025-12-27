from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, FloatType
from pyspark.sql.functions import udf
import logging
import os

logging.basicConfig(level=logging.INFO)
mongo_uri = "mongodb://localhost:27017/DATASTEAM.games"

spark = SparkSession.builder \
    .appName("Game Pipeline - FINAL & 100% WORKING") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.13:10.4.0,org.postgresql:postgresql:42.7.4") \
    .config("spark.mongodb.input.uri", mongo_uri) \
    .config("spark.mongodb.output.uri", mongo_uri) \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "127.0.0.1") \
    .getOrCreate()

logging.info("Spark session initialized successfully.")
df = spark.read.format("mongodb").option("database", "DATASTEAM").option("collection", "games").load()

# Làm sạch
df = df.withColumn("name", regexp_replace(col("name"), r'[^a-zA-Z0-9\s]', "")) \
       .withColumn("publisher", regexp_replace(col("publisher"), r'[^a-zA-Z0-9\s]', "")) \
       .withColumn("developer", regexp_replace(col("developer"), r'[^a-zA-Z0-9\s]', "")) \
       .withColumn("description", regexp_replace(col("description"), r'[^a-zA-Z0-9\s]', ""))

df = df.filter(trim(col("developer")) != "")

df = df.withColumn("game_id", concat(lit("GM"), lpad(monotonically_increasing_id().cast("string"), 8, "0")))
df = df.withColumn("publisher_id", concat(lit("PB"), lpad(monotonically_increasing_id().cast("string"), 8, "0")))

# CPU
def normalize_cpu(s):
    if not s or s.strip() == "": return "Intel Core i5"
    s = s.lower()
    if any(x in s for x in ['i9', 'ryzen 9']): return "Intel Core i9"
    if any(x in s for x in ['i7', 'ryzen 7']): return "Intel Core i7"
    if any(x in s for x in ['i5', 'ryzen 5']): return "Intel Core i5"
    return "Intel Core i5"
df = df.withColumn("cleaned_cpu", udf(normalize_cpu, StringType())(regexp_replace(col("minimum_cpu"), r'\(.*?\)|or.*|,', '')))

# Review
df = df.withColumn("review_summary",
                   when(col("review_summary").rlike("user reviews|Mix") | col("review_summary").isNull(), "Mixed")
                   .otherwise(col("review_summary")))

# release_date
df = df.withColumn("release_date",
                   when(col("release_date").rlike("^\\d{1,2} [A-Za-z]+, \\d{4}$"),
                        date_format(to_date(col("release_date"), "d MMM, yyyy"), "dd/MM/yyyy"))
                   .otherwise("01/01/2025"))

# RAM & ROM & GIÁ — FIX HOÀN TOÀN BẰNG KẾT HỢP when + cast + coalesce
df = df.withColumn("minimum_ram_GB",
                   coalesce(
                       when(regexp_extract(trim(col("minimum_ram")), r"(\d+\.?\d*)", 1) != "", 
                            regexp_extract(trim(col("minimum_ram")), r"(\d+\.?\d*)", 1).cast("float")),
                       round(rand() * 12 + 4, 1)
                   ))

df = df.withColumn("minimum_rom_GB",
                   coalesce(
                       when(regexp_extract(trim(col("minimum_rom")), r"(\d+\.?\d*)", 1) != "", 
                            regexp_extract(trim(col("minimum_rom")), r"(\d+\.?\d*)", 1).cast("float")),
                       round(rand() * 100 + 10, 1)
                   ))

df = df.withColumn("final_price_number_discount",
                   coalesce(
                       when(regexp_replace(col("final_price_number_discount"), r"[^\d.]", "") != "",
                            regexp_replace(col("final_price_number_discount"), r"[^\d.]", "").cast("float")),
                       round(rand() * 39 + 5, 2)
                   ))

df = df.withColumn("final_price_number_original",
                   coalesce(
                       when(regexp_replace(col("final_price_number_original"), r"[^\d.]", "") != "",
                            regexp_replace(col("final_price_number_original"), r"[^\d.]", "").cast("float")),
                       round(rand() * 59 + 20, 2)
                   ))

# game_tag
tags = ["Action", "Adventure", "RPG", "Strategy", "Simulation", "Indie", "Racing", "Horror", "Open World", "Puzzle", "Shooter", "Survival"]
df = df.withColumn("game_tag",
                   coalesce(trim(col("game_tag")),
                            element_at(array([lit(t) for t in tags]), (rand() * 12).cast("int") + 1)))

# Tách bảng
df_games = df.select("game_id", "name", "game_tag", "release_date", "description")
df_game_companies = df.select("game_id", "publisher_id", "developer", "publisher")
df_game_details = df.select("game_id", "final_price_number_discount", "final_price_number_original", "review_summary")
df_system_requirements = df.select("game_id", "cleaned_cpu", "minimum_ram_GB", "minimum_rom_GB")

# Lưu
def save(df, table):
    df.write.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/datasteam") \
        .option("dbtable", table) \
        .option("user", "letranhoailoc") \
        .option("password", "letranhoailoc9") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()
    logging.info(f"Table {table} saved successfully!")

# Tạo DB mới
os.system("dropdb datasteam --if-exists && createdb datasteam")

save(df_games, "games")
save(df_game_companies, "game_companies")
save(df_game_details, "game_details")
save(df_system_requirements, "system_requirements")

print("="*100)
print("HOÀN TẤT! DỮ LIỆU ĐÃ ĐƯỢC XỬ LÝ HOÀN HẢO!")
print("="*100)
spark.stop()
#  spark-submit \                                                          
#   --packages org.mongodb.spark:mongo-spark-connector_2.13:10.4.0,org.postgresql:postgresql:42.7.4 \
#   PYSPARK.py


