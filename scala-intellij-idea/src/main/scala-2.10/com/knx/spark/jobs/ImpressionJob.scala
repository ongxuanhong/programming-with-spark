package com.knx.spark.jobs

/**
  * Created by hongong on 5/3/16.
  */

import com.knx.spark.schema.ImpressionLog
import com.mongodb.casbah.{WriteConcern => MongodbWriteConcern}
import com.stratio.datasource.mongodb._
import com.stratio.datasource.mongodb.config.MongodbConfig._
import com.stratio.datasource.mongodb.config.MongodbConfigBuilder
import org.apache.spark.sql.functions.{udf, _}

object ImpressionJob extends BaseJob {

  /*
  * Default settings
  * */

  def hourTS(s: Long) = s - s % 3600

  val hourTs = udf(hourTS(_: Long))

  // Config collection ad_unit
  val adUnitBuilder = MongodbConfigBuilder(Map(Host -> JobSetting.configConnection,
    Database -> JobSetting.CONFIG_DB,
    Collection -> JobSetting.AD_UNIT_COLLECTION_NAME,
    SamplingRatio -> 1.0))

  // Config collection adunit_log
  val adUnitLogBuilder = MongodbConfigBuilder(Map(Host -> JobSetting.configConnection,
    Database -> JobSetting.CONFIG_DB,
    Collection -> JobSetting.AD_UNIT_LOG_COLLECTION_NAME,
    SamplingRatio -> 1.0))

  // Config collection ads
  val aDPublisherBuilder = MongodbConfigBuilder(
    Map(Host -> JobSetting.configConnection,
      Database -> JobSetting.CONFIG_DB,
      Collection -> JobSetting.MOBILE_COLLECTION_NAME,
      SamplingRatio -> 1.0))

  // Config collection bd
  val bDPublisherBuilder = MongodbConfigBuilder(
    Map(Host -> JobSetting.configConnection,
      Database -> JobSetting.CONFIG_DB,
      Collection -> JobSetting.BD_COLLECTION_NAME,
      SamplingRatio -> 1.0))

  // Build configurations
  val adUnitConf = adUnitBuilder.build()
  val adUnitLogConf = adUnitLogBuilder.build()
  val adConf = aDPublisherBuilder.build()
  val bdConf = bDPublisherBuilder.build()


  /*
  * Processing pageViewCount
  * */

  override def process(): Unit = {

    val outColl: String = "%s_%02d_%s".format(JobSetting.FINAL_OUT_COLLECTION, utcDate.getMonthOfYear, utcDate.getYear)
    val rawDB: String = getRawDbName()
    val pageViewColl: String = getPageViewCollName() + s"_$startEpoch"

    println("Raw DB : %s".format(rawDB))
    println("Date range %s - %s".format(startEpoch, endEpoch))
    println("Pageview collection : %s".format(pageViewColl))
    println("Out collection : %s".format(outColl))

    // Config collection brand_display_mm_yyyy
    val outBuilder = MongodbConfigBuilder(Map(Host -> JobSetting.outConnection,
      Database -> JobSetting.OUT_STATISTICS_DB,
      Collection -> outColl,
      WriteConcern -> "safe",
      SamplingRatio -> 1.0,
      UpdateFields -> Array("widgetId", "section", "date", "publisher",
        "os", "device", "browser"
      )))

    // Config collection pageview_dd_timestamp
    val impressionBuilder = MongodbConfigBuilder(Map(Host -> JobSetting.rawConnection,
      Database -> rawDB,
      Collection -> pageViewColl))

    // Build configurations
    val imConf = impressionBuilder.build()
    val outConf = outBuilder.build()

    // Loading collection into DataFrame by using builded configurations
    val adUnitDF = sqlContext.fromMongoDB(adUnitConf).withColumn("adUnitActive", col("isActive")).withColumn("aName", col("name"))
    val adUnitLogDF = sqlContext.fromMongoDB(adUnitLogConf).withColumn("adUnitLogActive", col("isActive")).withColumn("logName", col("name"))

    //   process for ADS collection not implement yet.
    val adDF = sqlContext.fromMongoDB(adConf)
    val bdDF = sqlContext.fromMongoDB(bdConf)
    val impressionDF = sqlContext.fromMongoDB(imConf, Some(ImpressionLog.schema)).filter(col("delayed") === 0)

    /*
  * Processing for main widgetId
  * */

    val allKindOfBdDF = bdDF.selectExpr("code as bdCode", "ref_id", "publisher as bdPublisher")
    val allAdunitDF = adUnitLogDF.join(adUnitDF, col("adunit_key") === col("key") && col("adUnitLogActive") === true && col("adUnitActive") === true)
    val bdDataOtherPublisher = impressionDF
      .join(allAdunitDF, col("widgetId") === col("widget_id"))
      .groupBy(col("widgetId"), col("url"), col("referer"), col("extras"), col("time"),
        col("os"), col("device"), col("browser"), col("publisher"))
      .agg(count(col("widgetId")).as("count"))

    val bdDataMainPublisher = impressionDF
      .join(allKindOfBdDF, col("bdCode") === col("widgetId"))
      .groupBy(col("widgetId"), col("url"), col("referer"), col("extras"), col("time"),
        col("os"), col("device"), col("browser"), col("bdPublisher").as("publisher"))
      .agg(count(col("widgetId")).as("count"))


    /*
  * Processing for widget is ref_id
  *
  * */

    val refIdDataOtherPublisher = impressionDF
      .join(allKindOfBdDF, col("ref_id") === col("widgetId"))
      .join(allAdunitDF, col("bdCode") === col("widget_id"))
      .groupBy(col("bdCode").as("widgetId"), col("url"), col("referer"), col("extras"), col("time"),
        col("os"), col("device"), col("browser"), col("publisher"))
      .agg(count(col("widgetId")).as("count"))

    val refIdDataMainPublisher = impressionDF
      .join(allKindOfBdDF, col("ref_id") === col("widgetId"))
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


    // TODO: enable when everything ok
    val sectionDF = rawInputDF.select(
      col("widgetId"), hourTs(col("time")).as("date"), coalesce(col("extras.adunit")).as("section"), col("publisher"), col("count"),
      expr("null").as("os"), expr("null").as("device"), expr("null").as("browser"))
      .groupBy("widgetId", "date", "publisher", "section", "os", "device", "browser")
      .agg(sum("count").as("pageViewCount"))

    val overallDF = rawInputDF.select(
      col("widgetId"), hourTs(col("time")).as("date"), expr("null").as("section"), col("publisher"), col("count"),
      expr("null").as("os"), expr("null").as("device"), expr("null").as("browser"))
      .groupBy("widgetId", "date", "publisher", "section", "os", "device", "browser")
      .agg(sum("count").as("pageViewCount"))

    //    get columns to order when union all
    val columns = breakDownOsDeviceOverallDF.columns.toSet.intersect(breakDownOsDeviceDF.columns.toSet).map(col).toSeq
    val breakDownDF = breakDownOsDeviceOverallDF.select(columns: _*).unionAll(breakDownOsDeviceDF.select(columns: _*))

    // Save result to brand_display_mm_yyyy
    println("Save DataFrame results to brand_display_mm_yyyy")
    breakDownDF.saveToMongodb(outConf)

  }

  def stopProgress(): Unit = {
    sc.stop()
  }
}