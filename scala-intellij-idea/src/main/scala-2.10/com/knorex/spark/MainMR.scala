/**
  * Created by hongong on 4/29/16.
  */

import com.stratio.datasource.mongodb._
import com.stratio.datasource.mongodb.config._
import com.stratio.datasource.mongodb.config.MongodbConfig._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object MainMR extends App {

  override def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Knorex Spark MapReduce Application").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val builder = MongodbConfigBuilder(Map(
      Host -> List("localhost:27017"),
      Database -> "brand_display_analytics",
      Collection -> "brand_display_04_2016",
      SamplingRatio -> 1.0,
      WriteConcern -> "normal"))
    val readConfig = builder.build()
    val mongoRDD = sqlContext.fromMongoDB(readConfig)
    mongoRDD.registerTempTable("brand_display_04_2016")
    sqlContext.sql("select * from brand_display_04_2016").show()

    System.out.println("OK")
  }
}