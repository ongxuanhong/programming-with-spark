package com.knorex.spark

/**
  * Created by hongong on 5/3/16.
  */

import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}


class CommandRunUtils() {

  case class RunParams(startDate: String = "", endDate: String = "", widgetIds: String = "", automated: Boolean = true)

  case class ConfigHourException(msg: String) extends Exception

  def getInputFromParams(args: Array[String]): (DateTime, DateTime, List[String]) = {
    var startDate = new DateTime()
    var endDate = new DateTime()
    var widgetIds = List[String]()

    val parser = new scopt.OptionParser[RunParams]("scopt") {
      head("logs-processing", "0.1")
      opt[String]("startDate") action { (x, c) =>
        c.copy(startDate = x)
      } text ("startDate is an String property format yyyy-MM-dd HH:MM:SS")
      opt[String]("endDate") action { (x, c) =>
        c.copy(endDate = x)
      } text ("endDate is an String property format yyyy-MM-dd HH:MM:SS")
      opt[String]("widgetIds") action { (x, c) =>
        c.copy(widgetIds = x)
      } text ("widgetIds is an String property format wid_1,wid_2")
    }
    // parser.parse returns Option[C]
    parser.parse(args, RunParams()) match {
      case Some(config) =>

        // widget ids params
        if (config.widgetIds.length() > 0) {
          widgetIds = config.widgetIds.split(",").toList
        }

        // datetime params
        if (config.startDate.isEmpty && config.automated) {
          val currentDateTime = DateTime.now()
          startDate = currentDateTime.plusHours(-1).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0)
          endDate = startDate.plusHours(1)
        }
        if (config.startDate > config.endDate) {
          throw ConfigHourException("start date is after end date")
        }
        else {
          startDate = DateTime.parse(config.startDate, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))
          endDate = DateTime.parse(config.endDate, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))
        }

      case None =>
      // arguments are bad, error message will have been displayed
    }
    (startDate, endDate, widgetIds)
  }
}