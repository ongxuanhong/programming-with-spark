package com.knx.spark

/**
  * Created by hongong on 4/29/16.
  */

import com.knx.spark.jobs.{ImpressionJob, ImpressionJobTest, JobSetting}
import com.knx.spark.utils.CommandRunUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.joda.time.DateTimeZone

object MainExecutor extends App {

  override def main(args: Array[String]) {

    // Initialize SparkContext
    val sc = new SparkContext(JobSetting.sparkConf)
    val sqlContext = new SQLContext(sc)

    val util = new CommandRunUtils()
    var (startDate, endDate, widgetIds) = util.getInputFromParams(args)

    println("------------------------------------------")
    var utcDate = startDate.toDateTime(DateTimeZone.UTC)
    var utcEpoch = utcDate.getMillis / 1000
    var startEpoch = startDate.getMillis / 1000
    val endEpoch = endDate.getMillis / 1000
    println("Start date:" + startDate + "/Epoch:" + startEpoch)
    println("End date:" + endDate + "/Epoch:" + endEpoch)
    println("UTC date:" + utcDate + "/Epoch:" + utcEpoch)
    println("Widgets [" + widgetIds.length + "] " + widgetIds.mkString(","))
    println("------------------------------------------")

    while (startDate.isBefore(endDate)) {

      utcDate = startDate.toDateTime(DateTimeZone.UTC)
      utcEpoch = utcDate.getMillis / 1000
      startEpoch = startDate.getMillis / 1000

      ImpressionJobTest
        .setDate(utcDate)
        .setWidgetIds(widgetIds)
        .process(sqlContext)

      // Increase 1 hour
      startDate = startDate.plusHours(1)
      utcDate = utcDate.plusHours(1)
    }

    ImpressionJobTest.stopProgress(sc)
  }
}