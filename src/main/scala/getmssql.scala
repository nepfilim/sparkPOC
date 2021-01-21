import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object getmssql {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("getmssql").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val url = "jdbc:sqlserver://mdabdenmssql.ck6vgv6qae1n.us-east-2.rds.amazonaws.com:1433;databaseName=rafidb;"
    val df = spark.read.format("jdbc").option("user","msuername")
      .option("password","mspassword")
      .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver").option("dbtable","abhidf").option("url",url).load()
    df.show()

    spark.stop()
  }
}