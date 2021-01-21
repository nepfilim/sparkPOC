package com.sam.bigdadat.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object rddtoDF {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("rddtoDF").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val data = "D:\\bigdata\\datasets\\bank-full.csv"
    // years back old strategy
    val rdd = sc.textFile(data)
    val head = rdd.first() // header// age; balance, marital,job
    //val fields = head.split(";").map(x => StructField(x.replaceAll("\"",""), StringType, nullable = true))
    //val schema = StructType(fields)

    // Convert records of the RDD (people) to Rows
    val rowRDD = rdd.map(x=>x.replaceAll("\"","").split(";")).map(x => Row.fromSeq(x))

    // Apply the schema to the RDD
    //val df = spark.createDataFrame(rowRDD, schema)
    //df.show(5)


    spark.stop()
  }
}