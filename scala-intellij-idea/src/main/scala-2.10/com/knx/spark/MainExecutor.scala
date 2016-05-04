package com.knx.spark

/**
  * Created by hongong on 4/29/16.
  */

import com.knx.spark.jobs.ImpressionJob
import com.knx.spark.utils.CommandRunUtils
import org.joda.time.DateTimeZone

object MainExecutor extends App {

  override def main(args: Array[String]) {

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
    println("Widgets ["+ widgetIds.length +"] " + widgetIds.mkString(","))
    println("------------------------------------------")

    while (startDate.isBefore(endDate)) {

      utcDate = startDate.toDateTime(DateTimeZone.UTC)
      utcEpoch = utcDate.getMillis / 1000
      startEpoch = startDate.getMillis / 1000

      ImpressionJob
        .setDate(utcDate)
        .setWidgetIds(widgetIds)
        .process()

      // Increase 1 hour
      startDate = startDate.plusHours(1)
      utcDate = utcDate.plusHours(1)
    }

    ImpressionJob.stopProgress()
  }
}