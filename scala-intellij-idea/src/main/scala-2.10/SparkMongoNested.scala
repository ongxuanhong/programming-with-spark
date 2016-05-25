/**
  * Created by hongong on 5/23/16.
  */

import com.stratio.datasource.mongodb._
import com.stratio.datasource.mongodb.config.MongodbConfig._
import com.stratio.datasource.mongodb.config.MongodbConfigBuilder
import com.stratio.datasource.util.Config
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{udf, _}

object SparkMongoNested extends App {

  def configBuilder(host: List[String], db: String, collection: String): Config = {
    return MongodbConfigBuilder(
      Map(Host -> host,
        Database -> db,
        Collection -> collection,
        SamplingRatio -> 0.1))
      .build()
  }

  def hourTS(s: Long) = s - s % 3600

  val hourTs = udf(hourTS(_: Long))

  def getWhereClauseStudio(): String = {
    val whereClause: String = "os IS NOT NULL " +
      "AND device IS NOT NULL " +
      "AND url NOT LIKE 'file%' " +
      "AND url NOT LIKE 'http://localhost%' " +
      "AND url NOT LIKE 'https://localhost%' " +
      "AND widgetId = 'knxad_knx2991_201509231117'"

    whereClause
  }

  override def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Spark MongoDB Application").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val outConf = MongodbConfigBuilder(Map(Host -> "localhost:27017",
      Database -> "brand_display_analytics",
      Collection -> "bd_test_join",
      SamplingRatio -> 1.0,
      WriteConcern -> "safe"
    )).build()

    val analyticsInteractionConf = configBuilder("localhost:27017".split(",").toList, "analytics_05_2016", "interaction_03")
    val analyticsInteractionDF = sqlContext.fromMongoDB(analyticsInteractionConf)

    val studioInteractionConf = configBuilder("localhost:27017".split(",").toList, "studio_entry", "interaction")
    val studioInteractionDF = sqlContext.fromMongoDB(studioInteractionConf)

    // join two interactions for overall section
    studioInteractionDF.join(analyticsInteractionDF,
      studioInteractionDF.col("code") === analyticsInteractionDF.col("widgetId") &&
        studioInteractionDF.col("trigger") === analyticsInteractionDF.col("trigger") &&
        studioInteractionDF.col("object") === analyticsInteractionDF.col("object") &&
        studioInteractionDF.col("response") === analyticsInteractionDF.col("response"))
      .where(studioInteractionDF.col("code") === "knxad_knx2991_201509231117")
      .groupBy(studioInteractionDF.col("code"), studioInteractionDF.col("_id"), studioInteractionDF.col("clickthrough"))
      .agg(count(col("code")).as("count"))
      .select(studioInteractionDF.col("code"), studioInteractionDF.col("_id"), studioInteractionDF.col("clickthrough"), col("count"))
      .show(100)

    // join two interactions for each section
//    val interactionBreakDownArray = studioInteractionDF.join(analyticsInteractionDF,
//      studioInteractionDF.col("code") === analyticsInteractionDF.col("widgetId") &&
//        studioInteractionDF.col("trigger") === analyticsInteractionDF.col("trigger") &&
//        studioInteractionDF.col("object") === analyticsInteractionDF.col("object") &&
//        studioInteractionDF.col("response") === analyticsInteractionDF.col("response"))
//      .where(studioInteractionDF.col("code") === "knxad_knx2991_201509231117")
//      .select(studioInteractionDF.col("code"),
//        studioInteractionDF.col("_id"),
//        studioInteractionDF.col("clickthrough"),
//        analyticsInteractionDF.col("extras.adunit").as("section"))
//      .groupBy(studioInteractionDF.col("code"),
//        studioInteractionDF.col("_id"),
//        studioInteractionDF.col("clickthrough"),
//        col("section"))
//      .agg(count(col("code")).as("count"))
//      .collect()

  }

}

