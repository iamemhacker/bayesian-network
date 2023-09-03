package com.erbridge.ds

import org.apache.log4j.Logger
import org.apache.spark.sql.{SparkSession, functions => F, DataFrame, Dataset,
  Column}
import com.erbridge.ds.types._
import java.time.LocalDate

import com.erbridge.ds.features.Conversions
import com.erbridge.ds.network.{NetworkBuilder, NetworkQuery, Statistics,
  ChiSquareEntry}
import com.erbridge.ds.validation.{Oracle, Validation}
import com.erbridge.ds.types.ComponentDiscrete
// for piping syntax.
import Utils.chaining._


trait Runner {
  def run(spark: SparkSession, logger: Option[Logger]): Unit
}

/**
 * Factory for spark runnables.
 **/
object RunnerFactory {
  val featureNames = () => {
    // TODO: Implement.
    Array(
      "field1",
      "field2",
      "field3")
  }

  val featureCardinalities = () => {
    // TODO: implement.
    Array(1, 2, 3)
  }

  def getLogger = (logger: Option[Logger]) => logger.getOrElse(Logger.getRootLogger())


  /**
   * Feature extraction.
   **/
  def extractFeatures(inputPath: String, outputPath: String): Runner = {

    return new Runner {

      def run(spark: SparkSession, logger: Option[Logger]): Unit = {
        getLogger(logger).info(s"EM Running extractFeatures")
        val df = spark.read.csv(inputPath)
        df.show()
      }

    }
  }


  def buildNetwork(featuresInputPath: String,
                   modelOutputPath: String) : Runner = {
    return new Runner {

      def run(spark: SparkSession, logger: Option[Logger]): Unit = {
        import spark.implicits._

        println(s"""Running network builder with
          | featuresInputPath=${featuresInputPath}
          | modelOutputPath=${modelOutputPath}""".stripMargin('|'))

        val builder = NetworkBuilder.build(spark) _
        val dsFeatures = spark.read.parquet(featuresInputPath)
          .as[ComponentDiscrete]

        val graph = builder(featureNames(), featureCardinalities(), dsFeatures)
        println(s"Saving graph to ${modelOutputPath}")
        NetworkBuilder.save(spark)(graph, modelOutputPath)
      }
    }
  }

  def networkQuery(modelInputPath: String,
                   predictionInputPath: String,
                   probabilitiesOutputPath: String,
                   sampleRate: Option[Double]=None): Runner = {
    return new Runner {

      def run(spark: SparkSession, logger: Option[Logger]): Unit = {
        import spark.implicits._
        
        println(s""" Running network query with
          | modelInputPath=${modelInputPath}
          | predictionInputPath=${predictionInputPath}
          | probabilitiesOutputPath=${probabilitiesOutputPath}
          """.stripMargin('|'))
        val dsPred = spark
          .read
          .parquet(predictionInputPath)
          .sample(sampleRate.getOrElse(1.0))
          .as[ComponentDiscrete]
        val dsModel = NetworkQuery.fit(
          spark=spark,
          names=featureNames(),
          cardinalities=featureCardinalities(),
          cache=Some(modelInputPath))(data=null)
        NetworkQuery.transform(spark, dsModel)(dsPred)
          .write
          .mode("overwrite")
          .parquet(probabilitiesOutputPath)
        println(s"Bayesian probabilities written to ${probabilitiesOutputPath}")
      }
    }
  }

  def oracle(predictionInputPath: String,
             probabilitiesOutputPath: String): Runner = {
    return new Runner {
      def run(spark: SparkSession, logger: Option[Logger]): Unit = {
        import spark.implicits._

        println(s"""Running oracle with:
          | prectionInput=${predictionInputPath}
          | probabilitiesOutputPath=${probabilitiesOutputPath}"""
          .stripMargin('|'))
        val dsPred = spark
          .read
          .parquet(predictionInputPath)
          .as[ComponentDiscrete]

        val dsProbs = Oracle.calcProbabilities(spark, dsPred)
        dsProbs.write.mode("overwrite").parquet(probabilitiesOutputPath)
        println(s"Oracle probabilities written to ${probabilitiesOutputPath}")
      }
    }
  }

  def validation(networkProbabilitiesPath: String,
                 validationSetPath: String,
                 validationOutputPath: String): Runner = {

    return new Runner {
      def run(spark: SparkSession, logger: Option[Logger]): Unit = {
        import spark.implicits._

        println(s"""=== Running validation with ===
          | networkProbabilitiesPath=${networkProbabilitiesPath}
          | validationSetPath=${validationSetPath}
          | validationOutputPath=${validationOutputPath}"""
          .stripMargin('|'))

        val dsTrain = spark
          .read
          .parquet(networkProbabilitiesPath)
          .as[LabeledEntry]
        val dsValidation = spark
          .read
          .parquet(validationSetPath)
          .as[ComponentDiscrete]
        val dsScores = Validation.predict(spark)(dsTrain)

        dsScores
          .write
          .mode("overwrite")
          .parquet(validationOutputPath)
        //val dsSummary = Validation.summarize(spark)(dsPrediction, dsValidation)
      }
    }
  }

}
