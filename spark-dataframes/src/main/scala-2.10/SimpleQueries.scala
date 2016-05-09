/**
  * Created by hongong on 5/8/16.
  */

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.functions._

object SimpleQueries {

  def selectSimple(sqlContext: SQLContext, df: DataFrame): Unit = {
    // SELECT by sqlContext
    sqlContext.sql("SELECT CustomerName,City FROM Customers").show(5)
    // SELECT by DataFrames
    df.select("CustomerName", "City").show(5)

    // SELECT DISTINCT by sqlContext
    sqlContext.sql("SELECT DISTINCT City FROM Customers").show(5)
    // SELECT DISTINCT by DataFrames
    df.select("City").distinct().show(5)
  }

  def whereClauseSimple(sqlContext: SQLContext, df: DataFrame): Unit = {
    // WHERE clause
    sqlContext.sql(
      """
        SELECT * FROM Customers
        WHERE Country='Mexico'
      """).show(5)
    df.where("Country='Mexico'").show(5)
    df.filter(col("Country") === "Mexico").show(5)
  }

  def operatorsWhereClause(sqlContext: SQLContext, df: DataFrame): Unit = {
    // Operators in The WHERE Clause
    sqlContext.sql(
      """
        SELECT * FROM Customers
        WHERE CustomerID < 5
      """).show()
    df.where("CustomerID < 5").show()
    df.filter(col("CustomerID").lt(5)).show()
    df.filter(col("CustomerID") < 5).show()

    sqlContext.sql(
      """
        SELECT * FROM Customers
        WHERE CustomerID BETWEEN 1 AND 4
      """).show()
    df.where("CustomerID BETWEEN 1 AND 4").show()
    df.filter(col("CustomerID").between(1, 4)).show()

    sqlContext.sql(
      """
        SELECT * FROM Customers
        WHERE CustomerName LIKE '%ana%'
      """).show(5)
    df.where("CustomerName LIKE '%ana%'").show()
    df.filter(col("CustomerName").like("%ana%")).show()

    sqlContext.sql(
      """
        SELECT * FROM Customers
        WHERE CustomerID IN (1,3,5)
      """).show()
    df.where("CustomerID IN (1,3,5)").show()
    df.filter(col("CustomerID").isin(List(1, 3, 5): _*)).show()

    sqlContext.sql(
      """
        SELECT * FROM Customers
        WHERE CustomerID <> 3
      """).show(5)
    df.where("CustomerID <> 3").show(5)
    df.filter(col("CustomerID") !== 3).show(5)
    df.filter(col("CustomerID").notEqual(3)).show(5)
  }

  def othersQueries(sqlContext: SQLContext, df: DataFrame): Unit = {
    sqlContext.sql(
      """
        SELECT * FROM Customers
        WHERE Country='Germany'
        AND City='Berlin'
      """).show()
    df.where("Country='Germany' AND City='Berlin'").show()
    df.filter(col("Country") === "Germany" && col("City") === "Berlin").show()

    sqlContext.sql(
      """
        SELECT * FROM Customers
        WHERE City='Berlin'
        OR City='München'
      """).show()
    df.where("City='Berlin' OR City='München'").show()
    df.filter(col("City") === "Berlin" || col("City") === "München").show()

    sqlContext.sql(
      """
        SELECT * FROM Customers
        ORDER BY Country ASC, CustomerName DESC
      """).show(5)
    df.orderBy(col("Country").asc, col("CustomerName").desc).show(5)
  }

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

    // Let sqlContext know which table you gonna work with
    df.registerTempTable("Customers")

    selectSimple(sqlContext, df)
    whereClauseSimple(sqlContext, df)
    operatorsWhereClause(sqlContext, df)
    othersQueries(sqlContext, df)

  }
}