import org.apache.spark.sql.SparkSession
//Load the data into Spark Environment and create the basic rdd
val salesrdd = sc.textFile("Sales_no_header.csv")

//extract the needed column using map method

val salesrdd1 = salesrdd.map(x => (x.split(",")(0), x.split(",")(2).toFloat))

// retrieve desired mapped row with the correct index number
val salesrdd2 = salesrdd1.map(x => (x._1,x._2))

// aggregate the values containing sales
val salesrdd3 = salesrdd2.reduceByKey((x,y) => x+y)

//rank the sales in ascending order
val salesrdd4 = salesrdd3.sortBy(x => x._2, false)

//print to screen desired result on each row
salesrdd4.collect().foreach(println)


//RESULT
//==============
//(FB,1220.0)   //FB had the nhigest sales
//(MSFT,967.0)
//(GOOG,660.0)
