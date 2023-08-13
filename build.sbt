val baseSettings = Seq(
  organization := "com.agoda",
  scalaVersion := "2.12.11",
  version := "0.0.1",
resolvers ++= Seq(
    "agoda-maven-hkg" at "https://repo-hkg.agodadev.io/agoda-maven"
    /*"Artima Maven Repository" at "http://repo.artima.com/releases"*/
  ),
  fork in Test := true,
  javaOptions in Test += "-Xmx4G",
  concurrentRestrictions in Global += Tags.limit(Tags.Test, 1),
  updateOptions := updateOptions.value.withCachedResolution(true)

)

val assemblySettings = Seq(
  assemblyJarName in assembly := "assembly.jar"
)


val v = new {
  val sparkassembly = "3.0.1.0"    //latest is here: https://github.agodadev.io/dse/spark-assembly/releases
  val sparkutils    = "3.0.27-PRE"     //latest is here: https://github.agodadev.io/it-data-frameworks/spark_frameworks/releases
  val hadooputils   = "2.4.9"     //latest is here: https://github.agodadev.io/it-data-frameworks/common_frameworks/releases
  val environment   = "2.3.44"    //latest is here: https://github.agodadev.io/it-data-frameworks/common_frameworks/releases
  val commonutils   = "1.0.40"    //latest is here: https://github.agodadev.io/it-data-frameworks/common_frameworks/releases
  val scalatest     = "3.2.14"
  val scalamock     = "3.6.0"
  val hadoop = "3.2.0"
}

val baseDependencies = Seq(
  "com.agoda.ml"  %% "commonutils" % v.commonutils,
  "com.agoda.ml"  %% "commonutils" % v.commonutils % "test" classifier "tests",
  "com.agoda.ml"  %% "environment" % v.environment,
  "org.datanucleus" % "datanucleus-accessplatform-jdo-rdbms" % "3.2.9" % "test",
  "org.scalatest" %% "scalatest" % v.scalatest % "test",
  "org.scalamock" %% "scalamock-scalatest-support" % v.scalamock % "test"
)

val hadoopDependencies = Seq(
  "org.apache.hadoop" % "hadoop-common"                 % v.hadoop,
  "org.apache.hadoop" % "hadoop-mapreduce-client-core"  % v.hadoop
).map(d => d % "provided" excludeAll(ExclusionRule(organization = "com.fasterxml.jackson*")))


val sparkDependencies = baseDependencies ++ Seq(
  "com.agoda.ml" %% "hadooputils" % v.hadooputils,
  "com.agoda.ml" %% "hadooputils" % v.hadooputils % "test" classifier "tests",
  "com.agoda.ml" %% "sparkutils" % v.sparkutils,
  "com.agoda.ml" %% "sparkutils" % v.sparkutils % "test" classifier "tests",
  "com.agoda.ml" %% "sparkassembly" % v.sparkassembly % "provided"
) ++ hadoopDependencies


lazy val ltcPrediction = project
  .settings(baseSettings: _*)
  .enablePlugins(DeployPlugin)
  .settings(assemblySettings: _*)
  .settings(
    libraryDependencies ++= sparkDependencies,
    name := "ltcPrediction"
  )
