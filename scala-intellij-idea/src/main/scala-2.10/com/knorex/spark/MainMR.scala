/**
  * Created by hongong on 4/29/16.
  */

import com.knorex.spark.CommandRunUtils

object MainMR extends App {

  override def main(args: Array[String]) {

    val util = new CommandRunUtils()
    val (startDate, endDate, widgetIds) = util.getInputFromParams(args)
    println(startDate)
    println(endDate)
    widgetIds.foreach(println)
  }
}