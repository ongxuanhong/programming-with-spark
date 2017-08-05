import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hongong on 6/28/16.
  */

object TuningSpark {

  def main_old (args : Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("Tuning Spark")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    // Data Serialization

  }

}