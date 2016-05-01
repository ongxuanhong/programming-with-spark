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

object MainMR extends App {

  override def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("Knorex Spark MapReduce Application")
      .setMaster("local[2]")
      .set("spark.executor.memory", "2g")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val bdConfig = MongodbConfigBuilder(Map(
      Host -> List("localhost:27017"),
      Database -> "brand_display_analytics",
      Collection -> "brand_display_04_2016",
      SamplingRatio -> 1.0,
      WriteConcern -> "normal")).build()

    val bdDF = sqlContext.fromMongoDB(bdConfig)
    bdDF.select(col("widgetId"), col("date")).show()

    System.out.println("OK")
  }
}