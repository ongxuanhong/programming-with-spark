/**
  * Created by hongong on 5/7/16.
  */

import com.stratio.datasource.mongodb._
import com.stratio.datasource.mongodb.config._
import com.stratio.datasource.mongodb.config.MongodbConfig._
import org.apache.spark.sql.{SQLContext, SaveMode}
import com.stratio.datasource.util.Config
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._

object SparkMongoToParquet extends App {

  def buildMongoConfig(host : String, database : String, collection : String) : Config = {
    val mongoConfig = MongodbConfigBuilder(Map(
      Host -> List(host),
      Database -> database,
      Collection -> collection,
      SamplingRatio -> 1.0,
      WriteConcern -> "normal")).build()
    return mongoConfig
  }

  override def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("Spark Mongo To Parquet")
      .setMaster("local[2]")
      .set("spark.executor.memory", "2g")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val host = "localhost:27017"
    val database = "analytics_04_2016"
    val collection = "pageview_09_1460160000"
    val saveParquetLocaltion = "/Users/hongong/Desktop/" + collection

    val mongodbConfig = buildMongoConfig(host, database, collection)
    println("*** Build Mongo Config")
    println("Host:" + host)
    println("Database:" + database)
    println("Collection:" + collection)

    val collectionDF = sqlContext.fromMongoDB(mongodbConfig)
    println("Reading into DataFrame")
    collectionDF.show(5)

    println("Write data into Parquet format")
    collectionDF.write.mode(SaveMode.Overwrite).format("json").save(saveParquetLocaltion)
    sc.stop()

  }
}