import org.apache.spark.sql.SparkSession

val newcond1 = sc.textFile("Etu.rtf")

val newcond2 = newcond1.flatMap(x => x.split(" "))

val newcond3 = newcond2.map(x => (x,1))

val newcond4 = newcond3.reduceByKey((x,y) => x+y)

newcond4.collect()
