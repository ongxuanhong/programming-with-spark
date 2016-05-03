/**
  * Created by hongong on 4/29/16.
  */

import com.knorex.spark.CommandRunUtils

object MainMR extends App {

  override def main(args: Array[String]) {

    val util = new CommandRunUtils()
    var (startDate, endDate, widgetIds) = util.getInputFromParams(args)
    widgetIds.foreach(println)

    while (startDate.isBefore(endDate)) {
      val startEpoch = startDate.getMillis / 1000
      val endEpoch = endDate.getMillis / 1000
      println("Start date:" + startDate + "/Epoch:" + startEpoch)
      println("End date:" + endDate + "/Epoch:" + endEpoch)

      // Increase 1 hour
      startDate = startDate.plusHours(1)
    }
  }
}