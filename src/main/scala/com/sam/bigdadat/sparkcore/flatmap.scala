package com.sam.bigdadat.sparkcore

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object flatmap {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("flatmap").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
   val data = "C:\\work\\datasets\\wcdata.txt"
    val urdd = sc.textFile(data)
    //val res = urdd.flatMap(x=>x.split("")).map
    val res = urdd.flatMap(x=>x.split(""))
       res.take(num = 5).foreach(println)
     // take: bases specified num display on top num of lines.
     // urdd.collect.foreach(println)
    //it is like group by value use redusebykey ..like select * frm
     spark.stop()
  }
}