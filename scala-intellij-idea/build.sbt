name := "programming with spark"

version := "1.0"

scalaVersion := "2.10.5"

unmanagedJars in Compile ++= Seq(
  file("lib/mongo-hadoop-core-1.5.2.jar"),
  file("lib/mongo-hadoop-spark-1.5.2.jar"),
  file("lib/mongo-java-driver-3.2.2.jar"),
  file("lib/spark-mongodb_2.10-0.11.1.jar")
)

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.6.1",
  "org.apache.spark" % "spark-sql_2.10" % "1.6.1",
  "org.mongodb" %% "casbah" % "3.1.1"
)