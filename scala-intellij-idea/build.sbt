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
  "org.mongodb" %% "casbah" % "3.1.1",
  "com.github.scopt" %% "scopt" % "3.4.0"
)

resolvers ++= Seq(
  "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository",
  "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
  "Spray Repository" at "http://repo.spray.cc/",
  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Twitter4J Repository" at "http://twitter4j.org/maven2/",
  "Apache HBase" at "https://repository.apache.org/content/repositories/releases",
  "Twitter Maven Repo" at "http://maven.twttr.com/",
  "scala-tools" at "https://oss.sonatype.org/content/groups/scala-tools",
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
  "Mesosphere Public Repository" at "http://downloads.mesosphere.io/maven",
  "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven",
  "Sonatype OSS Releases" at "https://oss.sonatype.org/content/repositories/releases/",
  Resolver.sonatypeRepo("public")
)

// set an explicit main class
mainClass in assembly := Some("com.knx.spark.MainExecutor")

// Configure JAR used with the assembly plug-in
assemblyJarName in assembly := "spark_aggregate_os.jar"

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.startsWith("META-INF") => MergeStrategy.discard
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.first
  case PathList("org", "apache", xs@_*) => MergeStrategy.first
  case PathList("org", "jboss", xs@_*) => MergeStrategy.first
  case "about.html" => MergeStrategy.rename
  case "reference.conf" => MergeStrategy.concat
  case "logback.xml" => MergeStrategy.concat
  case _ => MergeStrategy.first
}
