/**
  * Created by hongong on 4/25/16.
  */

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkContext, SparkConf}
import org.bson.BSONObject
import com.mongodb.BasicDBObject
import com.mongodb.hadoop.{MongoInputFormat, MongoOutputFormat}
import com.mongodb.hadoop.io.MongoUpdateWritable

object SparkMongoDemo extends App {

  override def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Spark MongoDB Application").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // Set up the configuration for reading from MongoDB.
    val mongoConfig = new Configuration()
    // MongoInputFormat allows us to read from a live MongoDB instance.
    // We could also use BSONFileInputFormat to read BSON snapshots.
    // MongoDB connection string naming a collection to read.
    // If using BSON, use "mapred.input.dir" to configure the directory
    // where the BSON files are located instead.
    mongoConfig.set("mongo.input.uri",
      "mongodb://localhost:27017/my_db_test.my_collection_test")

    // Create an RDD backed by the MongoDB collection.
    val documents = sc.newAPIHadoopRDD(
      mongoConfig, // Configuration
      classOf[MongoInputFormat], // InputFormat
      classOf[Object], // Key type
      classOf[BSONObject]) // Value type

    // Create a separate Configuration for saving data back to MongoDB.
    val outputConfig = new Configuration()
    outputConfig.set("mongo.output.uri", "mongodb://localhost:27017/my_db_test.my_out_collection_test")

    // We can choose to update documents in an existing collection by using the
    // MongoUpdateWritable class instead of BSONObject. First, we have to create
    // the update operations we want to perform by mapping them across our current
    // RDD.
    val updates = documents.mapValues(
      value => new MongoUpdateWritable(
        new BasicDBObject("_id", value.get("_id")), // Query
        new BasicDBObject("$set", new BasicDBObject("Michael", "Tommy")), // Update operation
        true, // Upsert
        false // Update multiple documents
      )
    )

    // Now we call saveAsNewAPIHadoopFile, using MongoUpdateWritable as the
    // value class.
    updates.saveAsNewAPIHadoopFile(
      "file:///this-is-completely-unused",
      classOf[Object],
      classOf[MongoUpdateWritable],
      classOf[MongoOutputFormat[Object, MongoUpdateWritable]],
      outputConfig)

  }
}

