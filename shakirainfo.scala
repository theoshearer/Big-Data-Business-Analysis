import org.apache.spark.sql.SparkSession

val salesrdd = sc.textFile("Sales_no_header.csv")

val salesrdd1 = salesrdd.map(x => (x.split(",")(0), x.split(",")(2).toFloat))

val salesrdd2 = salesrdd1.map(x => (x._1,x._2))

val salesrdd3 = salesrdd2.reduceByKey((x,y) => x+y)

val salesrdd4 = salesrdd3.sortBy(x => x._2, false)

salesrdd4.collect().foreach(println)


//RESULT
==============
(FB,1220.0)
(MSFT,967.0)
(GOOG,660.0)
