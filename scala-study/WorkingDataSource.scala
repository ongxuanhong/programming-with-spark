import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hongong on 6/28/16.
  */

object WorkingDataSource {

  def main_old (args : Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("Working data source")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    /**
      * Generic Load/Save Functions
      */
    var df = sqlContext.read.load("users.parquet")
    df.printSchema()
    df.collect().foreach(println)
//    df.select("name", "favorite_color").write.save("namesAndFavColors.parquet")

    // DataFrames of any type can be converted into other types
    df = sqlContext.read.format("json").load("people.json")
//    df.select("name", "age").write.mode(SaveMode.Overwrite).format("parquet").save("namesAndAges.parquet")

    // Run SQL on files directly
    sqlContext.sql("SELECT * FROM parquet.`users.parquet`").collect().foreach(println)




    /**
      * Parquet Files
      */

    // This is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._
    val people = sc.parallelize(Array(Person("Lucky", 15), Person("Rainy", 14))).toDF()

    // The RDD is implicitly converted to a DataFrame by implicits, allowing it to be stored using Parquet.
    people.write.mode(SaveMode.Overwrite).parquet("people.parquet")

    // Read in the parquet file created above. Parquet files are self-describing so the schema is preserved.
    // The result of loading a Parquet file is also a DataFrame.
    val parquetFile = sqlContext.read.parquet("people.parquet")

    //Parquet files can also be registered as tables and then used in SQL statements.
    parquetFile.registerTempTable("parquetFile")
    val teenagers = sqlContext.sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <= 19")
    teenagers.map(t => "Name: " + t(0)).collect().foreach(println)


  }

}