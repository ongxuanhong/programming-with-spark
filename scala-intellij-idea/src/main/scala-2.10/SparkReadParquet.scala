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

object SparkReadParquet extends App {

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

    val collection = "pageview_09_1460160000"
    val saveParquetLocaltion = "/Users/hongong/Desktop/" + collection
    val pageViewDF = sqlContext.read.parquet(saveParquetLocaltion)
    pageViewDF.show(5)

    sc.stop()

  }
}