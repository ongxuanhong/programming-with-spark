# Compile program
scalac -classpath "lib/spark-core_2.10-1.6.1.jar:lib/mongo-java-driver-3.2.2.jar:lib/mongo-hadoop-core-1.5.2.jar:mongo-hadoop-spark-1.5.2.jar:$SPARK_HOME/lib/spark-assembly-1.6.1-hadoop2.6.0.jar" \
SparkMongoDemo.scala \

# Create a JAR
jar -cvf spark_mongo_demo.jar SparkMongoDemo*.class \
lib/spark-core_2.10-1.6.1.jar \
lib/mongo-java-driver-3.2.2.jar \
lib/mongo-hadoop-core-1.5.2.jar \
lib/mongo-hadoop-spark-1.5.2.jar \
$SPARK_HOME/lib/spark-assembly-1.6.1-hadoop2.6.0.jar \

# Submit spark application
spark-submit --class SparkMongoDemo \
--master local[2] \
--jars lib/mongo-hadoop-spark-1.5.2.jar \
spark_mongo_demo.jar