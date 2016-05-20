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

object ImpressionJob extends BaseJob {

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
  val adUnitConf = configBuilder(JobSetting.configConnection, JobSetting.CONFIG_DB, JobSetting.AD_UNIT_COLLECTION_NAME)
  val adUnitLogConf = configBuilder(JobSetting.configConnection, JobSetting.CONFIG_DB, JobSetting.AD_UNIT_LOG_COLLECTION_NAME)
  val adConf = configBuilder(JobSetting.configConnection, JobSetting.CONFIG_DB, JobSetting.MOBILE_COLLECTION_NAME)
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

    // Loading ad_unit collection into DataFrame
    val adUnitDF = sqlContext.fromMongoDB(adUnitConf)
      .withColumnRenamed("isActive", "adUnitActive")
      .withColumnRenamed("name", "aName")
      .select("key", "adUnitActive", "aName", "publisher")

    // Loading adunit_log collection into DataFrame
    val adUnitLogDF = sqlContext.fromMongoDB(adUnitLogConf)
      .withColumnRenamed("isActive", "adUnitLogActive")
      .withColumnRenamed("name", "logName")
      .select("adunit_key", "widget_id", "adUnitLogActive", "logName")

    //   process for ADS collection not implement yet.
    val bdDF = sqlContext.fromMongoDB(bdConf)
      .withColumnRenamed("code", "bdCode")
      .withColumnRenamed("publisher", "bdPublisher")
      .select("bdCode", "bdPublisher", "ref_id")
    val impressionDF = sqlContext.fromMongoDB(pageViewConf, Some(ImpressionLog.schema)).filter(col("delayed") === 0)

    /*
  * Processing for main widgetId
  * */
    val allAdunitDF = adUnitLogDF.join(adUnitDF, col("adunit_key") === col("key") && col("adUnitLogActive") === true && col("adUnitActive") === true)
    val bdDataOtherPublisher = impressionDF
      .join(allAdunitDF, col("widgetId") === col("widget_id"))
      .groupBy(col("widgetId"), col("url"), col("referer"), col("extras"), col("time"),
        col("os"), col("device"), col("browser"), col("publisher"))
      .agg(count(col("widgetId")).as("count"))

    val bdDataMainPublisher = impressionDF
      .join(bdDF, col("bdCode") === col("widgetId"))
      .groupBy(col("widgetId"), col("url"), col("referer"), col("extras"), col("time"),
        col("os"), col("device"), col("browser"), col("bdPublisher").as("publisher"))
      .agg(count(col("widgetId")).as("count"))


    /*
  * Processing for widget is ref_id
  *
  * */

    val refIdDataOtherPublisher = impressionDF
      .join(bdDF, col("ref_id") === col("widgetId"))
      .join(allAdunitDF, col("bdCode") === col("widget_id"))
      .groupBy(col("bdCode").as("widgetId"), col("url"), col("referer"), col("extras"), col("time"),
        col("os"), col("device"), col("browser"), col("publisher"))
      .agg(count(col("widgetId")).as("count"))

    val refIdDataMainPublisher = impressionDF
      .join(bdDF, col("ref_id") === col("widgetId"))
      .groupBy(col("bdCode").as("widgetId"), col("url"), col("referer"), col("extras"), col("time"),
        col("os"), col("device"), col("browser"), col("bdPublisher").as("publisher"))
      .agg(count(col("widgetId")).as("count"))

    val refIdDataDF = refIdDataOtherPublisher.unionAll(refIdDataMainPublisher)
    val whereClause: String = getWhereClause

    val rawInputDF = bdDataOtherPublisher.unionAll(bdDataMainPublisher)
      .unionAll(refIdDataDF)
      .where(whereClause)

    val breakDownOsDeviceDF = rawInputDF.select(
      col("widgetId"), hourTs(col("time")).as("date"), col("extras.adunit").as("section"), col("publisher"),
      col("os"), col("device"), col("browser"), col("count"))
      .groupBy("widgetId", "date", "publisher", "section", "os", "device", "browser")
      .agg(sum("count").as("pageViewCount"))

    val breakDownOsDeviceOverallDF = rawInputDF.select(
      col("widgetId"), hourTs(col("time")).as("date"), expr("null").as("section"), col("publisher"),
      col("os"), col("device"), col("browser"), col("count"))
      .groupBy("widgetId", "date", "publisher", "section", "os", "device", "browser")
      .agg(sum("count").as("pageViewCount"))

    //    get columns to order when union all
    val columns = breakDownOsDeviceOverallDF.columns.toSet.intersect(breakDownOsDeviceDF.columns.toSet).map(col).toSeq
    val breakDownDF = breakDownOsDeviceOverallDF.select(columns: _*).unionAll(breakDownOsDeviceDF.select(columns: _*))

    // Save result to brand_display_mm_yyyy
    println("Save DataFrame results to brand_display_mm_yyyy")
    breakDownDF.saveToMongodb(outConf)

  }

  def stopProgress(sc: SparkContext): Unit = {
    sc.stop()
  }
}