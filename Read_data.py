from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import desc
from pyspark.sql.functions import trim

spark = SparkSession.builder.appName("RetailETL").getOrCreate()
df1 = spark.read.csv(
    "/Users/porselvi/Documents/learning-notes/Retail_ETL/Store.csv",
    header=True,
    inferSchema=True
)
df1.show()
df2 = spark.read.csv(
    "/Users/porselvi/Documents/learning-notes/Retail_ETL/product.csv",
    header=True
)
df2.show()

customer_df = spark.read.csv(
    "/Users/porselvi/Documents/learning-notes/Retail_ETL/customer.csv",
    header=True , inferSchema=True
)
customer_df.show()
customer_df.filter(
    col("CustomerName").isNull() |
    (trim(col("CustomerName")) == "")
).show()
customer_df.printSchema()
customer_df.show(truncate=False)

df2.show()
df2.printSchema()
rows = df2.count()
print (rows)

clean_df2 = df2.dropDuplicates(["ProductName"])
clean_df2.show()
clean_df3 = clean_df2.dropna(subset=["Price"])
clean_df3.show()
neg_value = clean_df2.filter(col("Price")< 0)
neg_value.show()
clean_df4 = clean_df3.replace("-12000", "12000")
clean_df4.show()
clean_df4_standard = clean_df4.withColumn(
    "Category",
    trim(col("Category"))
)
clean_df4_standard.show()
