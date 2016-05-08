/**
  * Created by hongong on 5/8/16.
  */

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object MainExecutor {
  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("Spark DataFrames Application")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("db/csv/customers.csv")

    df.show(5)

  }
}