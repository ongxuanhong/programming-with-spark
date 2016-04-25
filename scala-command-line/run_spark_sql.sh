# Compile program
scalac -classpath "lib/spark-core_2.10-1.6.1.jar:$SPARK_HOME/lib/spark-assembly-1.6.1-hadoop2.6.0.jar" \
SparkSQLDemo.scala \

# Create a JAR
jar -cvf spark_sql_demo.jar SparkSQLDemo*.class \
lib/spark-core_2.10-1.6.1.jar \
$SPARK_HOME/lib/spark-assembly-1.6.1-hadoop2.6.0.jar \

# Submit spark application
spark-submit --class SparkSQLDemo \
--master local[2] spark_sql_demo.jar