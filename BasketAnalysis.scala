import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

val df = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv("banking.csv")

df.printSchema()

df.show()
import spark.implicits._

// Success and failure rate

(df.filter($"y"==="yes").count()*1.0/df.count())*100

df.select(mean($"age")).show()
df.select(max($"age")).show()
df.select(min($"age")).show()

df.groupBy("y").show()
df.count()
df.groupBy("y").count().show()


df.groupBy("age").mean().show()
df.describe().show()

df.groupBy("balance").median().show()

df.groupBy("balance").agg(percentile_approx("balance",0.5,lit(10000000).alias("median"))

val df2 = df.select("age", "y")
df2.orderBy("age").show()
val df3 = df.groupBy("age", "y").count().where($"y"==="yes").sort()
df3.orderBy("y").show()

df.groupBy("age","y").count().where('y === "yes").orderBy('count.desc).show

df.select($"age", $"marital").filter('y=== "yes").groupBy($"age", $"marital").count.orderBy('count.desc).show()
val df4 = df.groupBy("marital", "y").count().where($"y"==="yes").orderBy('count.desc).show()
df4.orderBy("y").show()

val interval = 25
df.withColumn("range", $"age" - ($"age" % interval)).filter('y==="yes").count()
val df4 = df.groupBy("marital", "age").count().where($"y"==="yes").orderBy('count.desc).show()
