package com.sam.bigdadat.sparkcore

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object reducebykey {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("reducebykey").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext


    import spark.implicits._
    import spark.sql
    val csvdata = "C:\\work\\datasets\\asl.csv"
    val csvrdd = sc.textFile(csvdata)
    val head = csvrdd.first()
    val res = csvrdd.filter(x=>x!=head).map(x=>x.split(",")).map(x=>(x(2),1)).reduceByKey((x,y)=>x+y)
    res.foreach(println)
    // def reduceByKey(func : scala.Function2[V, V, V]) : org.apache.spark.rdd.RDD[scala.Tuple2[K, V]] = { /* compiled code */ }
    spark.stop()
  }
}
