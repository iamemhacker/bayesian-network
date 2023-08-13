package com.agoda.ds

import org.apache.spark.sql.{SparkSession, functions => F, DataFrame, Dataset,
  Column}
import com.agoda.ds.types._
import java.time.LocalDate

import com.agoda.ds.features.Conversions
import com.agoda.ds.network.{NetworkBuilder, NetworkQuery, Statistics,
  ChiSquareEntry}
import com.agoda.ds.validation.{Oracle, Validation}
import com.agoda.ds.types.ComponentDiscrete
// for piping syntax.
import Utils.chaining._


trait Runner {
  def run(spark: SparkSession): Unit
}

/**
 * Factory for spark runnables.
 **/
object RunnerFactory {

  /**
   * Feature extraction.
   **/
  def featuresExtractionLTC(tablesApi: TablesAPI,
                            featuresOutPath: String,
                            dtBeg: String,
                            dtEnd: String,
                            sampleRate: Option[Double]=None): Runner = {

    return new Runner {
      def filterByDT(spark: SparkSession, dfBensMatching: DataFrame):
        DataFrame = {
        import spark.implicits._

        val colDT = F.col("clickdate")
        dfBensMatching.where(colDT >= F.lit(dtBeg) && colDT <= F.lit(dtEnd))
      }

      def filterByModelID(df: DataFrame): DataFrame = {
        df.filter(F.col("model_id") === F.lit(23))
      }

      def run(spark: SparkSession): Unit = {
        import spark.implicits._

        println(s"""Running feature extraction with:
          | featuresOutPath=${featuresOutPath}
          | dtBeg=${dtBeg}
          | dtEnd=${dtEnd}
          | sampleRate=${sampleRate}""".stripMargin('|'))
        val dfHpaMatching = tablesApi
          .hpaBensMatching()
          .transform(Utils.bind(spark, filterByDT))
          .transform(filterByModelID)
        val dfMatchingWithLTC = Conversions.withLTC(dfHpaMatching)
        val dfHotels = tablesApi.hotels()
        val dfPricingFeatures = Conversions.hotelTraits(
          dfMatchingWithLTC,
          dfHotels)
          .select(Conversions.pricingColumns():_*)
          .na.fill("N/A")
          .as[MatchingFeatures]

        val sample = (ds: Dataset[ComponentDiscrete]) => {
          if (sampleRate.isEmpty)
            ds
          else
            ds.sample(withReplacement=false, fraction=sampleRate.get)
        }

        val dfFeatures = Conversions
          .pricingBayesianComponent(spark, dfPricingFeatures)
          .transform(sample)

        dfFeatures
          .write
          .mode("overwrite")
          .parquet(s"${featuresOutPath}/dt=${dtBeg}-${dtEnd}/")
      }
    }
  }

  implicit val featureNames = TypeUtils
          .getFieldOrder(MatchingFeatures())
          .sortBy(p => p._2)
          .map(p => p._1) :+ "LTC"

  implicit val featureCardinalities = TypeUtils
    .getFieldCardinality(MatchingFeatures())
    .sortBy(p => p._1)
    .map(p => p._2) :+ Conversions.NUM_LTC_BUCKETS

  def buildNetwork(featuresInputPath: String,
                   modelOutputPath: String) : Runner = {

    return new Runner {

      def run(spark: SparkSession): Unit = {
        import spark.implicits._

        println(s"""Running network builder with
          | featuresInputPath=${featuresInputPath}
          | modelOutputPath=${modelOutputPath}""".stripMargin('|'))

        val builder = NetworkBuilder.build(spark) _
        val dsFeatures = spark.read.parquet(featuresInputPath)
          .as[ComponentDiscrete]

        val graph = builder(featureNames, featureCardinalities, dsFeatures)
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

      def run(spark: SparkSession): Unit = {
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
          names=featureNames,
          cardinalities=featureCardinalities,
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
      def run(spark: SparkSession): Unit = {
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
      def run(spark: SparkSession): Unit = {
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
