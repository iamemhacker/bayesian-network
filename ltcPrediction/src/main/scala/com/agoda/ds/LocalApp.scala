package com.agoda.ds

import org.apache.spark.sql.{SparkSession}

object LocalApp extends SparkLocal {
  def appName(): String = {
    "LTC-prediction DS"
  }

  def main(args: Array[String]): Unit = {
    val dbApi = new ApiFactory(spark).createLocal("_data")
    if (args.length < 1) {
      throw new IllegalArgumentException("run mode not provided")
    }
    val oracleProbabilities = "out/oracle-probabilities/"
    val networkProbabilities = "out/probabilities/"
    val predictionPath = "out/prediction"
    val featuresPath = "out/features/"
    val cmd = args(0)
    val runner = cmd match {
      case "features-extraction" => RunnerFactory.featuresExtractionLTC(
        tablesApi=dbApi,
        featuresOutPath=featuresPath,
        dtBeg="20220101",
        dtEnd="20240101")
      case "topology-builder" => RunnerFactory.buildNetwork(
        featuresInputPath="out/features/dt=20220101-20240101/",
        modelOutputPath="out/network/")
      case "network-query" => RunnerFactory.networkQuery(
        modelInputPath="out/network/*.json",
        predictionInputPath=predictionPath,
        probabilitiesOutputPath=networkProbabilities)
      case "oracle" => RunnerFactory.oracle(
        predictionInputPath=predictionPath,
        probabilitiesOutputPath=oracleProbabilities)
      case "validation" => RunnerFactory.validation(
        networkProbabilitiesPath=networkProbabilities,
        validationSetPath=featuresPath,
        validationOutputPath="out/validation/")
      case default =>  throw new Exception(s"Unknown command ${default}")
    }

    runner.run(spark)
  }
}
