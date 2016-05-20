package com.knx.spark.jobs

/**
  * Created by hongong on 5/3/16.
  */

import com.knx.spark.schema.ImpressionLog
import com.stratio.datasource.mongodb._
import com.stratio.datasource.mongodb.config.MongodbConfig._
import com.stratio.datasource.mongodb.config.MongodbConfigBuilder
import com.stratio.datasource.util.Config
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{udf, _}

object ImpressionJobTest extends BaseJob {

  /*
  * Default settings
  * */

  def configBuilder(host: List[String], db: String, collection: String): Config = {
    return MongodbConfigBuilder(
      Map(Host -> host,
        Database -> db,
        Collection -> collection,
        SamplingRatio -> 1.0))
      .build()
  }

  def hourTS(s: Long) = s - s % 3600

  val hourTs = udf(hourTS(_: Long))

  // Build configurations
  val adsConf = configBuilder(JobSetting.configConnection, JobSetting.CONFIG_DB, JobSetting.MOBILE_COLLECTION_NAME)
  val bdConf = configBuilder(JobSetting.configConnection, JobSetting.CONFIG_DB, JobSetting.BD_COLLECTION_NAME)


  /*
  * Processing pageViewCount
  * */

  override def process(sqlContext: SQLContext): Unit = {

    val outColl: String = "%s_%02d_%s".format(JobSetting.FINAL_OUT_COLLECTION, utcDate.getMonthOfYear, utcDate.getYear)
    val rawDB: String = getRawDbName()
    val pageViewColl: String = getPageViewCollName() + s"_$startEpoch"

    println("Raw DB : %s".format(rawDB))
    println("Date range %s - %s".format(startEpoch, endEpoch))
    println("Pageview collection : %s".format(pageViewColl))
    println("Out collection : %s".format(outColl))

    // Config collection brand_display_mm_yyyy
    val outConf = MongodbConfigBuilder(Map(Host -> JobSetting.outConnection,
      Database -> JobSetting.OUT_STATISTICS_DB,
      Collection -> outColl,
      WriteConcern -> "safe",
      SamplingRatio -> 1.0,
      UpdateFields -> Array("widgetId", "section", "date", "publisher",
        "os", "device", "browser"
      ))).build()

    // Config collection pageview_dd_timestamp
    val pageViewConf = configBuilder(JobSetting.rawConnection, rawDB, pageViewColl)

    //   process for ADS collection not implement yet.
    val adsDF = sqlContext.fromMongoDB(adsConf)
      .withColumnRenamed("code", "bdCode")
      .withColumnRenamed("publisher", "bdPublisher")
      .select("bdCode", "bdPublisher", "ref_id")
      .where(filterByWidgetId("bdCode"))

    val bdDF = sqlContext.fromMongoDB(bdConf)
      .withColumnRenamed("code", "bdCode")
      .withColumnRenamed("publisher", "bdPublisher")
      .select("bdCode", "bdPublisher", "ref_id")
      .where(filterByWidgetId("bdCode"))

    // get all widgetIds from ads and bd
    val allWidgetDF = adsDF.unionAll(bdDF)

    val pageViewDF = sqlContext.fromMongoDB(pageViewConf)
      .filter(col("delayed") === 0)
      .where(filterByWidgetId("widgetId"))

    /*
  * Processing for main widgetId and widgetId has ref_id
  * */

    // get all section
    val bdDataDF = pageViewDF
      .join(allWidgetDF, col("bdCode") === col("widgetId"))
      .groupBy(col("widgetId"), col("url"), col("referer"), col("section"), hourTs(col("time")).as("time"),
        col("os"), col("device"), col("browser"), col("bdPublisher").as("publisher"))
      .agg(count(col("widgetId")).as("count"))

    // get overall
    val bdOverallDataDF = pageViewDF
      .join(allWidgetDF, col("bdCode") === col("widgetId"))
      .groupBy(col("widgetId"), col("url"), col("referer"), expr("null").as("section"), hourTs(col("time")).as("time"),
        col("os"), col("device"), col("browser"), col("bdPublisher").as("publisher"))
      .agg(count(col("widgetId")).as("count"))

    val refIdOverallDataDF = pageViewDF
      .join(allWidgetDF, col("ref_id") === col("widgetId"))
      .groupBy(col("bdCode").as("widgetId"), col("url"), col("referer"), expr("null").as("section"), hourTs(col("time")).as("time"),
        col("os"), col("device"), col("browser"), col("bdPublisher").as("publisher"))
      .agg(count(col("widgetId")).as("count"))

    val overallDataDF = bdOverallDataDF
      .unionAll(refIdOverallDataDF)
      .where(getWhereClauseWithSection(false))

    val breakDownOsDeviceDF = bdDataDF.select(
      col("widgetId"), col("time").as("date"), col("section"), col("publisher"),
      col("os"), col("device"), col("browser"), col("count"))
      .groupBy("widgetId", "date", "publisher", "section", "os", "device", "browser")
      .agg(sum("count").as("pageViewCount"))

    val breakDownOsDeviceOverallDF = overallDataDF.select(
      col("widgetId"), col("time").as("date"), col("section"), col("publisher"),
      col("os"), col("device"), col("browser"), col("count"))
      .groupBy("widgetId", "date", "publisher", "section", "os", "device", "browser")
      .agg(sum("count").as("pageViewCount"))

    //    get columns to order when union all
    val columns = breakDownOsDeviceOverallDF.columns.toSet.intersect(breakDownOsDeviceDF.columns.toSet).map(col).toSeq

    val breakDownDF = breakDownOsDeviceOverallDF.select(columns: _*).unionAll(breakDownOsDeviceDF.select(columns: _*))

    // Save result to brand_display_mm_yyyy
    println("Save DataFrame results to brand_display_mm_yyyy")
    breakDownDF.show()
    breakDownDF.saveToMongodb(outConf)

  }

  def stopProgress(sc: SparkContext): Unit = {
    sc.stop()
  }
}