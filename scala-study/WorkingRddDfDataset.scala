import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hongong on 6/28/16.
  */

case class Person(name : String, age : Int)

object WorkingRddDfDataset {

  def main_old (args : Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("Working with RDD")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    /**
      * DataFrame Operations
      */

    val df = sqlContext.read.json("people.json")

    // Show the content of the DataFrame
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

    // Running SQL Queries Programmatically
    df.registerTempTable("people")
    sqlContext.sql(
      """
        |SELECT * FROM people
      """.stripMargin).collect().foreach(println)



    /**
      * Creating Datasets
      */

    // Encoders for most common types are automatically provided by importing sqlContext.implicits._
    val ds = Seq(1, 2, 3).toDS()
    ds.map(_ + 1).collect().foreach(println)

    // Encoders are also created for case classes.
    val ds_person = Seq(Person("Andy", 32)).toDS()
    ds_person.collect().foreach(p => println("Person:" + p.name + "," + p.age))




    /**
      * Inferring the Schema Using Reflection
      */

    // Create an RDD of Person objects and register it as a table.
    val people_txt = sc.textFile("people.txt")
      .map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt))
      .toDF()
    people_txt.registerTempTable("people_txt")

    // SQL statements can be run by using the sql methods provided by sqlContext.
    val teenagers = sqlContext.sql("SELECT name, age FROM people_txt WHERE age >= 13 AND age <= 19")

    // The results of SQL queries are DataFrames and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by field index:
    teenagers.map(t => "Name: " + t(0)).collect().foreach(println)

    // or by field name:
    teenagers.map(t => "Name: " + t.getAs[String]("name")).collect().foreach(println)

    // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
    teenagers.map(_.getValuesMap[Any](List("name", "age"))).collect().foreach(println)


    /**
      * Programmatically Specifying the Schema
      */

    val people_txt_schema = sc.textFile("people.txt")

    // The schema is encoded in a string
    val schemaString = "name age"

    // Generate the schema based on the string of schema
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    // Convert records of the RDD (people) to Rows.
    val rowRDD = people_txt_schema.map(_.split(",")).map(p => Row(p(0), p(1).trim))

    // Apply the schema to the RDD.
    val peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema)

    // Register the DataFrames as a table.
    peopleDataFrame.registerTempTable("people_schema")

    // SQL statements can be run by using the sql methods provided by sqlContext.
    val results = sqlContext.sql("SELECT name FROM people_schema")

    // The results of SQL queries are DataFrames and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by field index or by field name.
    results.map(t => "Name: " + t(0)).collect().foreach(println)

    // Alternatively, a DataFrame can be created for a JSON dataset represented by
    // an RDD[String] storing one JSON object per string.
    val anotherPeopleRDD = sc.parallelize(
      """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
    val anotherPeople = sqlContext.read.json(anotherPeopleRDD)


    println("Exit...")


  }

}