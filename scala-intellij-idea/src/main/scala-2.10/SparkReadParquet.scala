/**
  * Created by hongong on 5/7/16.
  */

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkReadParquet extends App {

  override def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("Spark Mongo To Parquet")
      .setMaster("local[2]")
      .set("spark.executor.memory", "2g")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val collection = "pageview_09_1460160000"
    val saveParquetLocaltion = "/Users/hongong/Desktop/" + collection
    val pageViewDF = sqlContext.read.parquet(saveParquetLocaltion)
    pageViewDF.show(5)

    sc.stop()

  }
}