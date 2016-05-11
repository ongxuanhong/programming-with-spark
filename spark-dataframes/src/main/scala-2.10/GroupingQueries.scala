/**
  * Created by hongong on 5/8/16.
  */

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, _}

object GroupingQueries {

  def joinQueries(sqlContext: SQLContext, customersDF: DataFrame, ordersDF: DataFrame, employeesDF: DataFrame, suppliersDF: DataFrame): Unit = {

    // Inner join, Equal join
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

    // Left join
    sqlContext.sql(
      """
        |SELECT Customers.CustomerName, Orders.OrderID
        |FROM Customers
        |LEFT JOIN Orders
        |ON Customers.CustomerID=Orders.CustomerID
        |ORDER BY Customers.CustomerName
      """.stripMargin).show()
    customersDF.join(ordersDF, Seq("CustomerID", "CustomerID"), "left")
      .select("CustomerName", "OrderID")
      .orderBy("CustomerName")
      .show()

    // Right join
    sqlContext.sql(
      """
        |SELECT Orders.OrderID, Employees.FirstName
        |FROM Orders
        |RIGHT JOIN Employees
        |ON Orders.EmployeeID=Employees.EmployeeID
        |ORDER BY Orders.OrderID
      """.stripMargin).show(196)
    ordersDF.join(employeesDF, ordersDF.col("EmployeeID") === employeesDF.col("EmployeeID"), "right")
      .select(ordersDF.col("OrderID"), employeesDF.col("FirstName"))
      .orderBy(ordersDF.col("OrderID"))
      .show(196)

    // Full join
    sqlContext.sql(
      """
        |SELECT Customers.CustomerName, Orders.OrderID
        |FROM Customers
        |FULL OUTER JOIN Orders
        |ON Customers.CustomerID=Orders.CustomerID
        |ORDER BY Customers.CustomerName
      """.stripMargin).show(196)
    customersDF.join(ordersDF, customersDF.col("CustomerID").equalTo(ordersDF.col("CustomerID")), "outer")
      .select("CustomerName", "OrderID")
      .orderBy("CustomerName")
      .show(196)

    // CARTESIAN JOIN or CROSS JOIN
    ordersDF.join(customersDF).show(5)

    // Union
    sqlContext.sql(
      """
        |SELECT City FROM Customers
        |UNION
        |SELECT City FROM Suppliers
        |ORDER BY City
      """.stripMargin).show(5)
    customersDF.select("City").unionAll(suppliersDF.select("City")).distinct().show(5)

    // Union all with duplicates
    sqlContext.sql(
      """
        |SELECT City FROM Customers
        |UNION ALL
        |SELECT City FROM Suppliers
        |ORDER BY City
      """.stripMargin).show(5)
    customersDF.select("City").unionAll(suppliersDF.select("City")).show(5)

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

    // Loading suppliers data
    val suppliersDF = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("db/csv/suppliers.csv")

    // Let sqlContext know which table you gonna work with
    customersDF.registerTempTable("Customers")
    ordersDF.registerTempTable("Orders")
    employeesDF.registerTempTable("Employees")
    suppliersDF.registerTempTable("Suppliers")

    joinQueries(sqlContext, customersDF, ordersDF, employeesDF, suppliersDF)
  }
}