/**
  * Created by hongong on 5/3/16.
  */

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf

object JobSetting {
  val log = Logger.getLogger(getClass.getName)
  log.setLevel(Level.DEBUG)

  //  Application configurations
  val conf = ConfigFactory.load()
  val appName = conf.getString("app.name")
  val master = conf.getString("cluster.master")
  val executorMemory = conf.getString("executor.memory")

  //  Server connection information includes host:port
  // Config: studio_entry
  // Out: brand_display_analytics
  // Raw: analytics
  val CONFIG_CONNECTION = conf.getString("SERVER.CONFIG_CONNECTION")
  val OUT_CONNECTION = conf.getString("SERVER.OUT_CONNECTION")
  val RAW_CONNECTION = conf.getString("SERVER.RAW_CONNECTION")

  // Information of config database
  val DB_CONFIG_HOST = conf.getString("SERVER.DB_CONFIG_HOST")
  val DB_CONFIG_PORT = conf.getString("SERVER.DB_CONFIG_PORT")

  // Information of out database
  val DB_OUT_STATISTIC_HOST = conf.getString("SERVER.DB_OUT_STATISTIC_HOST")
  val DB_OUT_STATISTIC_PORT = conf.getString("SERVER.DB_OUT_STATISTIC_PORT")

  // Information of raw database
  val DB_RAW_HOST = conf.getString("SERVER.DB_RAW_ANALYTICS_HOST")
  val DB_RAW_PORT = conf.getString("SERVER.DB_RAW_ANALYTICS_PORT")

  // Database name
  val RAW_LOG_DB = conf.getString("RAW_LOG_DB")
  val CONFIG_DB = conf.getString("CONFIG_DB")
  val OUT_STATISTICS_DB = conf.getString("OUT_STATISTICS_DB")

  // Collection name
  //  Mobile: ads
  //  Bd: bd
  //  Interaction: interaction
  //  Ad unit log: adunit_log
  //  Ad unit: ad_unit
  //  Third party mobile: mobile_setup
  //  Third party: web_setup
  val MOBILE_COLLECTION_NAME = conf.getString("BD_DATA.MOBILE_COLLECTION_NAME")
  val BD_COLLECTION_NAME = conf.getString("BD_DATA.BD_COLLECTION_NAME")
  val INTERACTION_COLLECTION_NAME = conf.getString("BD_DATA.INTERACTION_COLLECTION_NAME")
  val AD_UNIT_COLLECTION_NAME = conf.getString("BD_DATA.AD_UNIT_COLLECTION_NAME")
  val AD_UNIT_LOG_COLLECTION_NAME = conf.getString("BD_DATA.AD_UNIT_LOG_COLLECTION_NAME")
  val THIRD_PARTY_MOBILE_COLLECTION_NAME = conf.getString("BD_DATA.THIRD_PARTY_MOBILE_COLLECTION_NAME")
  val THIRD_PARTY_COLLECTION_NAME = conf.getString("BD_DATA.THIRD_PARTY_COLLECTION_NAME")

  // Raw collection name
  val PAGEVIEW_COLL = conf.getString("RAW.PAGEVIEW_COLLECTION")
  val INTERACTION_COLL = conf.getString("RAW.INTERACTION_COLLECTION")
  val FINAL_OUT_COLLECTION = conf.getString("OUT.FINAL_OUT_COLLECTION")

  // Some helpful information
  val EXCLUDED_TRIGGERS = conf.getString("EXCLUDED_TRIGGERS").split("\\,").map(_.trim).toList
  val TIMING_TRIGGERS = Map("mouseover1" -> 1, "mouseover2" -> 2)

  // Get list connections
  val configConnection: List[String] = CONFIG_CONNECTION.split(",").toList
  val rawConnection: List[String] = RAW_CONNECTION.split(",").toList
  val outConnection: List[String] = OUT_CONNECTION.split(",").toList


  val sparkConf = new SparkConf()
    .setMaster(master)
    .setAppName(appName)
    //    Enable write ahead logs for receivers.
    // All the input data received through receivers will be saved
    // to write ahead logs that will allow it to be recovered after driver failures
    .set("spark.streaming.receiver.writeAheadLog.enable", "true")
    //    Amount of memory to use per executor process,
    // in the same format as JVM memory strings
    .set("spark.executor.memory", executorMemory)
    .set("spark.akka.frameSize", "500")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
}
