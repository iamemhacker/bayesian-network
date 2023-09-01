val baseSettings = Seq(
  organization := "er-bridge.com",
  scalaVersion := "2.13.8",
  version := "0.0.1",
  javaOptions in Test += "-Xmx4G",
  concurrentRestrictions in Global += Tags.limit(Tags.Test, 1),
  updateOptions := updateOptions.value.withCachedResolution(true)
)

val v = new {
  val scalatest = "3.2.16"
  val hadoop = "3.2.0"
  val typesafe = "1.4.2"
  val spark = "3.4.1"
  val scalaLogging = "3.9.4"
}

val baseDependencies = Seq(
  "org.scalatest" %% "scalatest" % v.scalatest % "test",
)

val hadoopDependencies = Seq(
  "org.apache.hadoop" % "hadoop-common"                 % v.hadoop,
  "org.apache.hadoop" % "hadoop-mapreduce-client-core"  % v.hadoop
)

val spark = Seq(
  "org.apache.spark" %% "spark-core" % v.spark % Provided,
  "org.apache.spark" %% "spark-sql" % v.spark % Provided,
  "org.apache.spark" %% "spark-mllib" % v.spark % Provided
)

val misc = Seq(
  "com.typesafe" % "config" % v.typesafe,
  "com.typesafe.scala-logging" %% "scala-logging" % v.scalaLogging
)


lazy val bayesianNetwork = project
  .settings(baseSettings: _*)
  .settings(
    name := "bayesianNetwork",
    libraryDependencies ++= spark,
    libraryDependencies ++= misc
 )
