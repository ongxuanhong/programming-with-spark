# Compile program
scalac -classpath "lib/spark-core_2.10-1.6.1.jar:$SPARK_HOME/lib/spark-assembly-1.6.1-hadoop2.6.0.jar" \
SparkWordCount.scala \

# Create a JAR
jar -cvf wordcount.jar SparkWordCount*.class \
lib/spark-core_2.10-1.6.1.jar \
$SPARK_HOME/lib/spark-assembly-1.6.1-hadoop2.6.0.jar \

# Submit spark application
spark-submit --class SparkWordCount \
--master local[2] wordcount.jar