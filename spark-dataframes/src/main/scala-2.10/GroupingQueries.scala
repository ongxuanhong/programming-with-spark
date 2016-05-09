/**
  * Created by hongong on 5/8/16.
  */

import org.apache.spark.{SparkContext, _}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._

object GroupingQueries {

  def selectSimple(sqlContext: SQLContext, customersDF: DataFrame): Unit = {
    sqlContext.sql(
      """
        |SELECT * FROM Customers
        |LIMIT 5
      """.stripMargin).show()
    customersDF.limit(5).show()

    sqlContext.sql(
      """
        |SELECT CustomerName AS Customer, ContactName AS Contact_Person1
        |FROM Customers
      """.stripMargin).show(5)
    customersDF.withColumnRenamed("CustomerName", "Customer 2")
      .withColumnRenamed("ContactName", "Contact Person 2")
      .select("Customer 2", "Contact Person 2").show(5)
    customersDF.select(col("CustomerName").alias("Customer 3"), col("ContactName").alias("Contact Person 3")).show(5)

  }

  def innerJoinQueries(sqlContext: SQLContext, customersDF: DataFrame, ordersDF: DataFrame): Unit = {
    sqlContext.sql(
      """
        |SELECT Orders.OrderID, Customers.CustomerName, Orders.OrderDate
        |FROM Orders
        |INNER JOIN Customers
        |ON Orders.CustomerID=Customers.CustomerID
        |ORDER BY CustomerName ASC
      """.stripMargin).show()
    ordersDF.join(customersDF, "CustomerID")
      .select("OrderID", "CustomerName", "OrderDate")
      .orderBy("CustomerName")
      .show()

  }

  def leftJoinQueries(sqlContext: SQLContext, customersDF: DataFrame, ordersDF: DataFrame, employeesDF: DataFrame): Unit = {
    //    sqlContext.sql(
    //      """
    //        |SELECT Customers.CustomerName, Orders.OrderID
    //        |FROM Customers
    //        |LEFT JOIN Orders
    //        |ON Customers.CustomerID=Orders.CustomerID
    //        |ORDER BY Customers.CustomerName
    //      """.stripMargin).show()
    //    customersDF.join(ordersDF, Seq("CustomerID", "CustomerID"), "left")
    //      .select("CustomerName", "OrderID")
    //      .orderBy("CustomerName")
    //      .show()

    //    sqlContext.sql(
    //      """
    //        |SELECT Orders.OrderID, Employees.FirstName
    //        |FROM Orders
    //        |RIGHT JOIN Employees
    //        |ON Orders.EmployeeID=Employees.EmployeeID
    //        |ORDER BY Orders.OrderID
    //      """.stripMargin).show(196)
//    ordersDF.join(employeesDF, ordersDF.col("EmployeeID") === employeesDF.col("EmployeeID"), "right")
//      .select(ordersDF.col("OrderID"), employeesDF.col("FirstName"))
//      .orderBy(ordersDF.col("OrderID"))
//      .show(196)

//    sqlContext.sql(
//      """
//        |SELECT Customers.CustomerName, Orders.OrderID
//        |FROM Customers
//        |FULL OUTER JOIN Orders
//        |ON Customers.CustomerID=Orders.CustomerID
//        |ORDER BY Customers.CustomerName
//      """.stripMargin).show(196)
    customersDF.join(ordersDF, customersDF.col("CustomerID").equalTo(ordersDF.col("CustomerID")), "outer")
      .select("CustomerName", "OrderID")
      .orderBy("CustomerName")
      .show(196)
  }

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("Spark DataFrames Application")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // Loading customers data
    val customersDF = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("db/csv/customers.csv")

    // Loading orders data
    val ordersDF = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("db/csv/orders.csv")

    // Loading employees data
    val employeesDF = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("db/csv/employees.csv")

    // Let sqlContext know which table you gonna work with
    customersDF.registerTempTable("Customers")
    ordersDF.registerTempTable("Orders")
    employeesDF.registerTempTable("Employees")

    // selectSimple(sqlContext, customersDF)
    //    innerJoinQueries(sqlContext, customersDF, ordersDF)
    leftJoinQueries(sqlContext, customersDF, ordersDF, employeesDF)
  }
}