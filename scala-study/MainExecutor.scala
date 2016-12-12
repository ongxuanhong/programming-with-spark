import java.io.{BufferedReader, InputStreamReader}
import java.net.URL

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by hongong on 8/12/16.
  */


object MainExecutor {

  //  Function for getting bots
  var ip_bots = Array[String]()

  def get_ip_bots(): Array[String] = {
    println("Downloading webcrawler IPs")
    val webCrawlerListURL = "https://myip.ms/files/bots/live_webcrawlers.txt"
    val url = new URL(webCrawlerListURL)
    val in = new BufferedReader(new InputStreamReader(url.openStream))
    val buffer = new ArrayBuffer[String]()
    var inputLine = in.readLine

    println("File Downloaded. Reading the file now...")
    while (inputLine != null) {
      if (!inputLine.trim.equals("")) {
        buffer += inputLine.trim
      }
      inputLine = in.readLine
    }
    in.close

    println("File read. Processing the file")
    val ipAddressValidation = new IPAddressValidation()
    val ip_bots = buffer.filter(ip => {
      ipAddressValidation.ipValidation(ip)
    })

    ip_bots.toArray[String]
  }

  //  UDF for checking bots
  def checkBot(address: String): Boolean = {
    val ips = address.split(",")

    for (ip <- ips) {
      if (ip_bots contains ip.trim) {
        true
      }
    }

    false
  }

  val udfCheckBots = udf(checkBot(_: String))

  def main(args: Array[String]) {

    // Initialize Spark
    val spark = SparkSession.builder().getOrCreate()

    // Getting list bots
    ip_bots = get_ip_bots()

    // Summary
    var summary: Map[String, Long] = Map()

    // Default params
    val seconds_diff = 1000

    // 01 Nov 2016 GMT
    val startEpochTime = 1477958400
    // 05 Nov 2016 GMT
    val endEpochTime = 1478304000

    var click_time_map: Map[String, Long] = Map()

    import spark.implicits._
    var mongoInteractionDF = MongoSpark.load[Interaction](spark).as[Interaction] // Uses the SparkConf
    mongoInteractionDF = mongoInteractionDF.filter(col("time") >= startEpochTime && col("time") < endEpochTime)
    mongoInteractionDF = mongoInteractionDF.repartition(6, col("widgetId"), col("uid"), col("remoteAddr")).sortWithinPartitions()
    summary += ("interaction_before" -> mongoInteractionDF.count())

    // Split into 2 sets
    var clickThroughDF = mongoInteractionDF.filter(col("trigger") === "click" && col("extras.clickthrough") === true)
    val otherInteractionDF = mongoInteractionDF.filter(col("trigger").notEqual("click"))
    summary += ("clickthrough_before" -> clickThroughDF.count())

    // Filter bots
    var filterBots = clickThroughDF.withColumn("isBot", udfCheckBots(col("remoteAddr")))
    filterBots = filterBots.filter(col("isBot") === false)
    filterBots = filterBots.drop(col("isBot"))
    clickThroughDF = filterBots.as[Interaction]
    summary += ("clickthrough_after_bots" -> clickThroughDF.count())

    // De-duplicate by time
    clickThroughDF.createOrReplaceTempView("ClickThrough")
    var deDuplicate = spark.sql(
      """
        |SELECT widgetId, uid, remoteAddr, time, COUNT(time) as numDuplicate
        |FROM ClickThrough
        |GROUP BY widgetId, uid, remoteAddr, time
      """.stripMargin).filter(col("numDuplicate") === 1)
    deDuplicate.createOrReplaceTempView("DeDuplicate")
    summary += ("deduplicate_1" -> deDuplicate.count())

    // De-duplicate by seconds
    deDuplicate = deDuplicate.filter(x => {
      val remoteAddr = x.getAs[String]("remoteAddr")
      val uid = x.getAs[String]("uid")
      val time = x.getAs[Long]("time")
      val user = remoteAddr + "_" + uid
      if (click_time_map.contains(user) && Math.abs(click_time_map(user) - time) <= seconds_diff) {
        false
      } else {
        click_time_map += (user -> time)
        true
      }
    })
    summary += ("deduplicate_2" -> deDuplicate.count())

    deDuplicate.createOrReplaceTempView("DeDuplicate")
    deDuplicate = spark.sql(
      """
        |SELECT ClickThrough.kinesisId, ClickThrough.widgetId, ClickThrough.url, ClickThrough.referer,
        |ClickThrough.trigger, ClickThrough.object, ClickThrough.response, ClickThrough.extra, ClickThrough.agent,
        |ClickThrough.time, ClickThrough.uid, ClickThrough.extras, ClickThrough.remoteAddr, ClickThrough.os,
        |ClickThrough.device, ClickThrough.section, ClickThrough.browser, ClickThrough.interactionTime
        |
        |FROM ClickThrough
        |RIGHT JOIN DeDuplicate
        |ON ClickThrough.widgetId=DeDuplicate.widgetId
        |AND ClickThrough.uid=DeDuplicate.uid
        |AND ClickThrough.remoteAddr=DeDuplicate.remoteAddr
        |AND ClickThrough.time=DeDuplicate.time
      """.stripMargin)
    summary += ("joined" -> deDuplicate.count())

    // Union back
    clickThroughDF = deDuplicate.as[Interaction]
    val unionInteraction = clickThroughDF.union(otherInteractionDF)
    summary += ("interaction_after" -> unionInteraction.count())
    println("Summary", summary)

  }
}