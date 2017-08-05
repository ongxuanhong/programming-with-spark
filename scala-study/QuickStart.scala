import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hongong on 6/28/16.
  */

object QuickStart {

  def main_old (args : Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("Quick Start")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile("README.md")

    // Basics operations
    println("Number of items in this RDD:" + textFile.count())

    println("First item in this RDD:" + textFile.first())

    val linesWithSpark = textFile.filter(line => line.contains("spark"))
    linesWithSpark.collect().foreach(println)

    println("How many lines contain \"Spark\"?" + textFile.filter(line => line.contains("spark")).count())


    // More on RDD Operations
    val maxSize = textFile.map(line => line.split(" ").size)
      .reduce((a, b) => if (a > b) a else b)
    println("Max size:" + maxSize)

    val maxSizeMath = textFile.map(line => line.split(" ").size)
      .reduce((a, b) => Math.max(a, b))
    println("Max size use Math:" + maxSizeMath)

    // MapReduce, as popularized by Hadoop
    val wordCounts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey((a, b) => a + b).collect().foreach(println)

  }

}