/**
  * Created by hongong on 4/29/16.
  */

import com.stratio.datasource.mongodb._
import com.stratio.datasource.mongodb.config._
import com.stratio.datasource.mongodb.config.MongodbConfig._
import org.apache.spark.sql.SQLContext
import com.mongodb.casbah.{WriteConcern => MongodbWriteConcern}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.{udf, _}
import org.apache.parquet.format._

object MainMR extends App {

  override def main(args: Array[String]) {

    val t0 = System.nanoTime()


    val conf = new SparkConf()
      .setAppName("Knorex Spark MapReduce Application")
      .setMaster("local[2]")
      .set("spark.executor.memory", "2g")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val bdConfig = MongodbConfigBuilder(Map(
      Host -> List("localhost:27017"),
      Database -> "analytics_04_2016",
      Collection -> "pageview_09_fields",
      SamplingRatio -> 1.0,
      WriteConcern -> "normal")).build()

//        val bdDF = sqlContext.fromMongoDB(bdConfig)
//    val path = "/Users/hongong/Desktop/pageview_09.json"
    val path = "/Users/hongong/Desktop/pageview_09.parquet"
    val bdDF = sqlContext.read.parquet(path)
//    bdDF.write.parquet("/Users/hongong/Desktop/pageview_09.parquet")
    bdDF.show()

    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000000 + " seconds")
  }
}