package com.sam.bigdadat.sparkcore

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object hello {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("hello").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    spark.stop()
  }
}