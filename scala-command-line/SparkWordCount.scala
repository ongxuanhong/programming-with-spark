/**
  * Created by hongong on 4/20/16.
  */

import org.apache.spark._
import org.apache.spark.SparkContext

object SparkWordCount {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Word count Application").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val inputFile = sc.textFile("input.txt")
    val counts = inputFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_+_)

    /* saveAsTextFile method is an action that effects on the RDD */
    counts.saveAsTextFile("outfile")
    System.out.println("OK")
  }
}