import org.apache.spark.sql.SparkSession
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import ml.dmlc.xgboost4j.scala.Booster
import ml.dmlc.xgboost4j.scala.spark.XGBoost

/**
  * Created by hongong on 3/07/17.
  */

object SparkSQLDemo {
  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("Spark SQL Demo")
      .getOrCreate()

    val inputTrainPath = "/Users/hongong/Documents/hong_notebooks/python/data/agaricus.txt.train"
    val inputTestPath = "/Users/hongong/Documents/hong_notebooks/python/data/agaricus.txt.test"


	// create training and testing dataframes
    val numRound = 2
    // build dataset
    val trainDF = sparkSession.sqlContext.read.format("libsvm").load(inputTrainPath)
    val testDF = sparkSession.sqlContext.read.format("libsvm").load(inputTestPath)

    // start training
    val paramMap = List(
      "eta" -> 0.1f,
      "max_depth" -> 2,
      "objective" -> "binary:logistic").toMap
    val xgboostModel = XGBoost.trainWithDataFrame(trainDF, paramMap, numRound, nWorkers = 1)
    
    // xgboost-spark appends the column containing prediction results
	xgboostModel.transform(testDF).show()

  }
}
