package com.agoda.ds
import  com.agoda.ds.{ types => Ty }

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SparkSession, functions => F, DataFrame, Dataset}
import com.typesafe.config.{Config, ConfigFactory}

import com.agoda.ml.spark.SparkSqlMain
import com.typesafe.scalalogging.LazyLogging


object HadoopApp extends LazyLogging with SparkSqlMain {
  val featuresPath = "/user/AGODA+emalul/out/features/"
  val oraclePath = "/user/AGODA+emalul/out/oracle/"
  val modelPath = "/user/AGODA+emalul/out/model/"
  val probabilitiesPath = "/user/AGODA+emalul/out/probabilities/"

  def getDtWindow(config: Config): (String, String) = {
    (config.getString("deploy.dt-beg"), config.getString("deploy.dt-end"))
  }

  def pathFormat(path: String, dtBeg: String, dtEnd: String): String = {
    s"${path}/dt=${dtBeg}-${dtEnd}/"
  }

  def run(args: Array[String]): Unit = {
    val dbApi = new ApiFactory(spark).createRemote()
    val config = ConfigFactory.load()
    val runMode = config.getString("deploy.run-mode") 
    val (dtBeg, dtEnd) = getDtWindow(config)
    val pathDt = (path: String) => pathFormat(path, dtBeg, dtEnd)
  
    val runner = runMode match {
      case "features-extraction" => {
        val sampleRate = config.getDouble("deploy.sample-rate")
        RunnerFactory.featuresExtractionLTC(
        tablesApi=dbApi,
        featuresOutPath=featuresPath,
        dtBeg=dtBeg,
        dtEnd=dtEnd,
        sampleRate=Some(sampleRate))
      }
      case "topology-builder" => {
        RunnerFactory.buildNetwork(
          featuresInputPath=pathDt(featuresPath),
          modelOutputPath=pathDt(modelPath))
      }
      case "network-query" => {
        val sampleRate = config.getDouble("deploy.sample-rate")
        RunnerFactory.networkQuery(
          modelInputPath=modelPath,
          predictionInputPath=pathDt(featuresPath),
          probabilitiesOutputPath=pathDt(probabilitiesPath),
          sampleRate=Some(sampleRate))
      }
      case "oracle" => {
        val predictionInputPath = config
          .getString("deploy.prediction-input-path")
        val probabilitiesOutputPath = config
          .getString("deploy.probability-out-path")
        RunnerFactory.oracle(pathDt(featuresPath), pathDt(oraclePath))
      }
      case "validation" => {
        val configRegion = config.getString("deploy.region")
        val dest = if (configRegion == null) {
          "all"
        }  else {
          configRegion
        }
        val (dtTrainBeg, dtTrainEnd) = (
          config.getString("deploy.dt-train-beg"),
          config.getString("deploy.dt-train-end"))
        val netPredInputPath = config.getString("deploy.network-pred-input")
        val validationSetPath = config.getString("deploy.validtion-set-input")
        val validationOutputPath = config.getString("deploy.validation-output")
        RunnerFactory.validation(
          s"${pathFormat(netPredInputPath, dtTrainBeg, dtTrainEnd)}/dest=${dest}",
          pathDt(validationSetPath),
          pathDt(validationOutputPath))
      }
      case default => throw new
        IllegalArgumentException(s"Unknown run mode ${default}")
    }

    runner.run(spark)
  }
}
