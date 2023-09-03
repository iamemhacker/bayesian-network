package com.erbridge.ds

import org.apache.log4j.Logger
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SparkSession}

object LocalApp extends SparkLocal {
  def appName(): String = {
    "LTC-prediction DS"
  }

  def main(args: Array[String]): Unit = {
    val dataDir = config.getString("data.path")
    val rawPath = s"${dataDir}/raw/"
    val featuresPath = s"${dataDir}/features/"
    val predictionPath = s"${dataDir}/prediction"
    val oraclePath = s"${dataDir}/oracle/"
    val modelPath = s"${dataDir}/model/"
    val probabilitiesPath = s"${dataDir}/probabilities/"
    if (args.length < 1) {
      throw new Exception("invalid command line, missing run mode")
    }
    val cmd = args(0)

    val runner = cmd match {
      case "features-extraction" => { 
        RunnerFactory.extractFeatures(rawPath, featuresPath)
      }
      case "topology-builder" => RunnerFactory.buildNetwork(
        featuresInputPath="out/features/dt=20220101-20240101/",
        modelOutputPath="out/network/")
      case "network-query" => RunnerFactory.networkQuery(
        modelInputPath="out/network/*.json",
        predictionInputPath=predictionPath,
        probabilitiesOutputPath=probabilitiesPath)
      case "oracle" => RunnerFactory.oracle(
        predictionInputPath=predictionPath,
        probabilitiesOutputPath=oraclePath)
      case "validation" => RunnerFactory.validation(
        networkProbabilitiesPath=probabilitiesPath,
        validationSetPath=featuresPath,
        validationOutputPath="out/validation/")
      case default =>  throw new Exception(s"Unknown command ${default}")
    }

    runner.run(spark, Some(logger))
  }
}
