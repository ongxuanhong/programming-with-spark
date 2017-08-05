import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hongong on 6/28/16.
  */

object ProgrammingWithSpark {

  def main_old (args : Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("Programming with Spark")
    val sc = new SparkContext(conf)

    // Parallelized Collections
    val data = Array(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data)
    distData.collect().foreach(println)

    // RDD Operations
    val lines = sc.textFile("README.md")
    val lineLengths = lines.map(s => s.length)
    val totalLength = lineLengths.reduce((a, b) => a + b)
    println("Total characters:" + totalLength)

    // Closures
    val accum = sc.accumulator(0, "My Accumulator")
    distData.collect().foreach(x => accum += x)
    println("Accumulator value:" + accum.value)
  }

}