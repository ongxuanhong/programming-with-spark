package com.knx.spark.jobs

/**
  * Created by hongong on 5/3/16.
  */

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.joda.time.{DateTime, DateTimeZone}

trait BaseJob {

  // Initialize SparkContext
  val sc = new SparkContext(JobSetting.sparkConf)
  val sqlContext = new SQLContext(sc)

  // Initialize common attributes
  var startEpoch = 0L
  var endEpoch = 0L
  var widgetIds = List[String]()

  var utcDate = DateTime.now(DateTimeZone.UTC)
  private val SERVER_TIME_ZONE: String = "Asia/Singapore"
  var localDate = utcDate.withZone(DateTimeZone.forID(SERVER_TIME_ZONE))

  // Implement this function
  def process(): Unit = {

  }

  def setWidgetIds(widgetIds: List[String]): BaseJob = {
    this.widgetIds = widgetIds
    this
  }

  def setDate(date: DateTime): BaseJob = {
    this.utcDate = date
    this.localDate = utcDate.withZone(DateTimeZone.forID(SERVER_TIME_ZONE))
    startEpoch = localDate.getMillis / 1000
    endEpoch = localDate.plusHours(1).getMillis / 1000
    this
  }


  def getPageViewCollName(): String = {
    "%s_%02d".format(JobSetting.PAGEVIEW_COLL, utcDate.getDayOfMonth)
  }

  def getRawDbName(): String = {
    "%s_%02d_%s".format(JobSetting.RAW_LOG_DB, utcDate.getMonthOfYear, utcDate.getYear)
  }


  def getWhereClause: String = {
    var whereClause: String = "os IS NOT NULL " +
      "AND extras IS NOT NULL " +
      "AND browser IS NOT NULL " +
      "AND device IS NOT NULL " +
      "AND url NOT LIKE 'file%' " +
      s"AND time >= $startEpoch AND time < $endEpoch " +
      "AND url NOT LIKE 'http://localhost%' " +
      "AND url NOT LIKE 'https://localhost%'"

    if (widgetIds.nonEmpty) {
      if (widgetIds.length == 1) {
        whereClause += s" AND widgetId = '" + widgetIds(0) + "'"
      }
      else {
        whereClause += " AND widgetId IN (" + widgetIds.mkString("'","','", "'") + ")"
      }
    }
    whereClause
  }


}
