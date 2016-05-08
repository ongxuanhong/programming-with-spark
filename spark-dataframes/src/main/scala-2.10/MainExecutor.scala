/**
  * Created by hongong on 5/8/16.
  */

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object MainExecutor {
  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("Spark DataFrames Application")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // Loading customers data
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("db/csv/customers.csv")

    // SELECT statement
    df.registerTempTable("Customers")
    val selectColumns = sqlContext.sql("SELECT CustomerName,City FROM Customers")
    println("SELECT by sqlContext")
    selectColumns.show(5)

    println("SELECT by DataFrames")
    df.select("CustomerName", "City").show(5)

  }
}