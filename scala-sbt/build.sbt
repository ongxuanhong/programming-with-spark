name := "programming_with_spark"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.1.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.1.0"
)

// Configure JAR used with the assembly plug-in
assemblyJarName in assembly := "programming_with_spark.jar"

// A special option to exclude Scala itself form our assembly JAR, since Spark // already bundles Scala.
assemblyOption in assembly :=
  (assemblyOption in assembly).value.copy(includeScala = false)

resolvers ++= Seq(
  Resolver.sonatypeRepo("public"),
  Resolver.typesafeRepo("releases")
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false, includeDependency = false)

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

