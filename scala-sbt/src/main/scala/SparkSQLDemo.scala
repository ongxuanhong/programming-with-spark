import org.apache.spark.sql.SparkSession

/**
  * Created by hongong on 3/07/17.
  */

object SparkSQLDemo {
  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("Spark SQL Demo")
      .getOrCreate()

    val sqlContext = sparkSession.sqlContext
//    val filePath = getClass.getResource("people.json").getPath
    val df = sqlContext.read.json("./classes/people.json")

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