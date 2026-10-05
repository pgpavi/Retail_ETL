from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.window import Window
from pyspark.sql.functions import col
from pyspark.sql.functions import rank
from pyspark.sql.functions import desc
from pyspark.sql.functions import trim
from pyspark.sql.functions import upper
from pyspark.sql.functions import count
from pyspark.sql.functions import to_date
from pyspark.sql.functions import expr
from pyspark.sql.functions import abs
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
    DateType
)
from pyspark.sql.functions import (
    month,
    year,
    when,
    sum,
    mean,
    avg
)
builder = (
    SparkSession.builder
    .appName("Delta Demo")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
# Read data#
store_schema = StructType([
    StructField("StoreID", StringType(), True),
    StructField("StoreName", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True)
])
store_df = spark.read.csv(
    "/Users/porselvi/Documents/learning-notes/Retail_ETL/Store.csv",
    header=True, schema= store_schema)
store_df.printSchema()
store_rows = store_df.count()
print(store_rows)
store_df.show()

customer_df = spark.read.csv(
    "/Users/porselvi/Documents/learning-notes/Retail_ETL/Customer.csv",
    header=True,
    inferSchema=True, nullValue="NULL")
order_df = spark.read.csv(
    "/Users/porselvi/Documents/learning-notes/Retail_ETL/Order.csv",
    header=True,
    inferSchema=True)
product_df = spark.read.csv(
    "/Users/porselvi/Documents/learning-notes/Retail_ETL/Product.csv",
    header=True,
    inferSchema=True)
#Data Quality Checks#
#Customers#
customer_df= customer_df.withColumn("CustomerName", trim(col("CustomerName"))
                                    )
customer_df.show()
customer_df= customer_df.withColumn("CustomerName", upper(col("CustomerName")))
customer_df.show()
customer_df.filter(col("CustomerName").isNull()).show()
customer_df.filter(col("City").isNull()).show()
customer_df.groupBy("CustomerName", "City", "State", "SignupDate").count().show()
customer_df = customer_df.withColumn(
    "SignupDate_Converted",
   expr("try_to_date(SignupDate, 'yyyy-MM-dd')")
)
customer_df.show()
valid_customer_df = customer_df.filter(
    col("SignupDate_Converted").isNotNull())
valid_customer_df = valid_customer_df.drop("SignupDate")
valid_customer_df= valid_customer_df.withColumnRenamed(
    "SignupDate_Converted",
    "SignupDate"
)
valid_customer_df= valid_customer_df.dropDuplicates(["CustomerName"])
valid_customer_df.show()

#products#

product_df = product_df.dropDuplicates(["ProductName"])
product_df.show()
product_df.filter(col("Price").isNull()).show()
product_df.filter(col("Price") < 0).show()
product_df = product_df.withColumn ("Price", abs(col("Price")))
product_df = product_df.withColumn ("Category", upper(col("Category")))
product_df.show()
#stores#
store_df = store_df.dropDuplicates(["StoreID"])
store_df.show(truncate=False)
#orders#
order_df.show()
order_df.groupBy("OrderID").count().filter("count >1").show()
order_df = order_df.withColumn ("Quantity", abs(col("Quantity"))).withColumn ("UnitPrice", abs(col("UnitPrice")))
order_df.show()
order_df = order_df.withColumn(
    "OrderDate_Updated",
   expr("try_to_date(OrderDate, 'yyyy-MM-dd')")
)
order_df.show()
order_df = order_df.filter(
    col("OrderDate_Updated").isNotNull())
order_df= order_df.drop("OrderDate")
order_df= order_df.withColumnRenamed(
    "OrderDate_Updated",
    "OrderDate"
)
order_df.show()
invalid_customers = order_df.join(
    customer_df,
    on="CustomerID",
    how="left_anti"
)

invalid_customers.show()
invalid_product = order_df.join (product_df, on = "ProductID", how="left_anti")
invalid_product.show()
valid_orders = order_df \
    .join(customer_df.select("CustomerID"), on="CustomerID", how="inner") \
    .join(product_df.select("ProductID"), on="ProductID", how="inner") \
    .join(store_df.select("StoreID"), on="StoreID", how="inner")

valid_orders.show()
#Data Cleaning#
valid_customer_df = valid_customer_df.fillna({"CustomerName": "Unknown", "City": "Unknown"})
valid_customer_df.show()

#Transformation#
valid_orders_transformed_df = valid_orders \
    .join(customer_df, on="CustomerID", how="inner") \
    .join(product_df, on="ProductID", how="inner") \
    .join(store_df.select ("StoreID", "StoreName"), on="StoreID", how="inner")
valid_orders_transformed_df.show()
valid_orders_transformed_df = valid_orders_transformed_df.withColumn("TotalAmount", col ("Quantity") * col ("UnitPrice"))
valid_orders_transformed_df = valid_orders_transformed_df.withColumn(
    "OrderMonth",
    month(col("OrderDate"))).withColumn(
    "OrderYear",
    year(col("OrderDate")))

customer_spending =valid_orders_transformed_df.groupBy ("CustomerID").agg(sum ("TotalAmount").alias ("TotalSpending"))
customer_spending.show()
valid_orders_transformed_df = valid_orders_transformed_df.join(customer_spending, on = "CustomerID", how= "left")
valid_orders_transformed_df = valid_orders_transformed_df.withColumn ("CustomerType",
when(col("TotalSpending") > 50000, "Premium")
.otherwise ("Normal")
)                                                                    
valid_orders_transformed_df.show()
#Analytics#
revenue_by_state= valid_orders_transformed_df.groupBy("State").agg(sum("TotalAmount").alias ("TotalRevenue"))
revenue_by_state.orderBy(col("TotalRevenue").desc()).show()
top_products_sold = valid_orders_transformed_df.groupBy("ProductID", "ProductName").agg (sum ("Quantity").alias("QuantitySold"))\
.orderBy(col("QuantitySold").desc()).show()
avg_orderby_city = valid_orders_transformed_df.groupBy ("City").agg (avg ("TotalAmount").alias ("Avg_order")).orderBy (col ("Avg_order").desc()).show()
#window function - Rank#
customer_revenue = valid_orders_transformed_df.groupBy(
    "State",
    "CustomerID",
    "CustomerName"
).agg(
    sum("TotalAmount").alias("Revenue")
)
windowSpec = Window.partitionBy("State") \
                   .orderBy(col("Revenue").desc())
customer_rank = customer_revenue.withColumn ("Rank", rank().over(windowSpec))
customer_rank.show()
#Running total#
monthly_sales = valid_orders_transformed_df.groupBy ("OrderYear", "OrderMonth").agg (sum ("TotalAmount").alias("Montly_Revenue"))
windowsales = Window.orderBy ("OrderYear", "OrderMonth") 
running_sales = monthly_sales.withColumn ("RunningTotal", sum ("Montly_Revenue").over (windowsales.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
running_sales.show()
#sparksql - Top-Selling Products#
valid_orders_transformed_df.createOrReplaceTempView("orders")
spark.sql("select * from orders").show ()
spark.sql("""
SELECT
    ProductName,
    SUM(Quantity) AS TotalQuantity
FROM orders
GROUP BY ProductName
ORDER BY TotalQuantity DESC
""").show()
#premium customer#
spark.sql("""
SELECT
    CustomerID,
    CustomerName,
    SUM(TotalAmount) AS TotalSpending
FROM orders
GROUP BY
    CustomerID,
    CustomerName
HAVING SUM(TotalAmount) > 50000
ORDER BY TotalSpending DESC
""").show()
# Save Cleaned Tables to Parquet#
valid_orders_transformed_df.write \
    .mode("overwrite") \
    .parquet("Output/Silver/Orders")
#Save Gold Tables to Delta#
revenue_by_state.write \
    .format("delta") \
    .mode("overwrite") \
    .save("output/gold/revenue_by_state")
#Save Reports as CSV#
revenue_by_state.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("output/reports/revenue_by_state")


# Basic data validation
print("Total records:", customer_df.count())
print("Null Customer IDs:", customer_df.filter(col("CustomerID").isNull()).count())
