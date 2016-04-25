/**
  * Created by hongong on 4/23/16.
  */

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object SparkSQLDemo {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Spark SQL Application").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.json("people.json")

    // Displays the content of the DataFrame to stdout
    df.show()
    // Print the schema in a tree format
    df.printSchema()
    // Select only the "name" column
    df.select("name").show()
    // Select everybody, but increment the age by 1
    df.select(df("name"), df("age") + 1).show()
    // Select people older than 21
    df.filter(df("age") > 21).show()
    // Count people by age
    df.groupBy("age").count().show()
  }
}