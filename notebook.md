# Zeppelin notebook (PySpark) in readable format

%spark.pyspark
spark.version

%spark.pyspark
from pyspark.sql import functions as F
from pyspark.sql.window import Window

users = spark.createDataFrame(
[("u1","Berlin"),("u2","Berlin"),("u3","Munich"),("u4","Hamburg")],
["user_id","city"]
)

orders = spark.createDataFrame(
[("o1","u1","p1",2,10.0),
 ("o2","u1","p2",1,30.0),
 ("o3","u2","p1",1,10.0),
 ("o4","u2","p3",5,7.0),
 ("o5","u3","p2",3,30.0),
 ("o6","u3","p3",1,7.0),
 ("o7","u4","p1",10,10.0)],
["order_id","user_id","product_id","qty","price"]
)

products = spark.createDataFrame(
[("p1","Ring VOLA"),("p2","Ring POROG"),("p3","Ring TISHINA")],
["product_id","product_name"]
)

users.show()
orders.show()
products.show()

%spark.pyspark
orders2 = orders.withColumn("revenue", F.col("qty") * F.col("price"))
orders2.show()

%spark.pyspark
base = (orders2
        .join(users, on="user_id", how="left")
        .join(products, on="product_id", how="left"))
base.show()

%spark.pyspark
mart = (base
        .groupBy("city","product_id","product_name")
        .agg(
            F.countDistinct("order_id").alias("orders_cnt"),
            F.sum("qty").alias("qty_sum"),
            F.sum("revenue").alias("revenue_sum"),
        ))
mart.orderBy("city", F.desc("revenue_sum")).show(truncate=False)

%spark.pyspark
w = Window.partitionBy("city").orderBy(F.desc("revenue_sum"))

mart_top2 = (mart
             .withColumn("rn", F.row_number().over(w))
             .filter(F.col("rn") <= 2)
             .drop("rn"))

mart_top2.orderBy("city", F.desc("revenue_sum")).show(truncate=False)

%spark.pyspark
hdfs_path = "/tmp/sandbox_zeppelin/mart_city_top_products/"

(mart_top2.write
 .mode("overwrite")
 .parquet(hdfs_path))

 %spark.pyspark
spark.read.parquet(hdfs_path).show(truncate=False)

%spark.pyspark
s3_path = "s3a://hadoop/mart/mart_city_top_products/"

(mart_top2
 .coalesce(1)
 .write
 .mode("overwrite")
 .parquet(s3_path))

%spark.pyspark
spark.read.parquet(s3_path).show(truncate=False)


%spark.pyspark
mart_top2.explain(True)

%spark.pyspark
df = spark.read.parquet("s3a://hadoop/mart/mart_city_top_products/")
df.printSchema()

%spark.pyspark
df.count()

%spark.pyspark
df.groupBy("city").count().show()

%spark.pyspark
from pyspark.sql import functions as F
df.orderBy("city", F.desc("revenue_sum")).show(truncate=False)

%spark.pyspark
df.groupBy().agg(
    F.sum("orders_cnt").alias("total_orders"),
    F.sum("qty_sum").alias("total_qty"),
    F.sum("revenue_sum").alias("total_revenue")
).show()

%sh
hadoop fs -ls s3a://hadoop/mart/mart_city_top_products/

%spark.pyspark
df.describe().show()


