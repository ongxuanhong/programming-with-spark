import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hongong on 6/29/16.
  */

object WorkingRDD {

  // print out the contents of the RDD with partition labels
  def myfunc(index: Int, iter: Iterator[(Int)]) : Iterator[String] = {
    iter.toList.map(x => "[partID:" +  index + ", val: " + x + "]").iterator
  }

  def myfuncPair(index: Int, iter: Iterator[(String, Int)]) : Iterator[String] = {
    iter.toList.map(x => "[partID:" +  index + ", val: " + x + "]").iterator
  }

  def main (args : Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("Working RDD")
    val sc = new SparkContext(conf)

    /**
      * aggregate
      */

    val z = sc.parallelize(List(1,2,3,4,5,6), 2)
    z.mapPartitionsWithIndex(myfunc).collect.foreach(println)
    println("Max:" + z.aggregate(0)(math.max(_, _), _ + _))

    // This example returns 16 since the initial value is 5
    // reduce of partition 0 will be max(5, 1, 2, 3) = 5
    // reduce of partition 1 will be max(5, 4, 5, 6) = 6
    // final reduce across partitions will be 5 + 5 + 6 = 16
    // note the final reduce include the initial value
    println("Max initial:" + z.aggregate(5)(math.max(_, _), _ + _))

    val y = sc.parallelize(List("a","d","e","b","c","f"), 2)
    println("Concat string:" + y.aggregate("")(_ + _, _+_))

    // See here how the initial value "x" is applied three times.
    //  - once for each partition
    //  - once when combining all the partitions in the second reduce function.
    println("Concat string initial:" + y.aggregate("x")(_ + _, _+_))


    /**
      * aggregateByKey
      */

    val pairRDD = sc.parallelize(List( ("cat",2), ("cat", 5), ("mouse", 4),("cat", 12), ("dog", 12), ("mouse", 2)), 2)
    pairRDD.mapPartitionsWithIndex(myfuncPair).collect.foreach(println)
    pairRDD.aggregateByKey(0)(math.max(_, _), _ + _).collect.foreach(println)
    pairRDD.aggregateByKey(100)(math.max(_, _), _ + _).collect.foreach(println)


    /**
      * others
      */

    var c = sc.parallelize(List("Gnu", "Cat", "Rat", "Dog", "Gnu", "Rat"), 2)
    c.distinct.collect.foreach(println)
    println(c.first)

    var a = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10))
    a.distinct(2).partitions.length
    a.distinct(3).partitions.length

    // Similar to map, but allows emitting more than one item in the map function
    a = sc.parallelize(1 to 6, 2)
    a.flatMap(1 to _).collect.foreach(x => print(x + " "))

    // Aggregates the values of each partition.
    // The aggregation variable within each partition is initialized with zeroValue.
    a = sc.parallelize(List(1,2,3), 3)
    a.fold(0)(_ + _)

    // Executes an parameterless function for each data item.
    c = sc.parallelize(List("cat", "dog", "tiger", "lion", "gnu", "crocodile", "ant", "whale", "dolphin", "spider"), 3)
    c.foreach(x => println(x + "s are yummy"))

    // Executes an parameterless function for each partition.
    // Access to the data items contained in the partition is provided via the iterator argument.
    var b = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 3)
    b.foreachPartition(x => println(x.reduce(_ + _)))
    println(b.getStorageLevel)

    // Assembles an array that contains all elements of the partition and embeds it in an RDD.
    // Each returned array contains the contents of one partition.
    a = sc.parallelize(1 to 100, 3)
    a.glom.collect


  }

}