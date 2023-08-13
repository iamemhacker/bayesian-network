package com.agoda.ds.network

import org.apache.spark.sql.{Dataset, SparkSession, functions => F}
import org.apache.commons.math3.distribution.ChiSquaredDistribution
import org.apache.spark.ml.linalg.Vector

case class ChiSquareEntry(nullHypo: Int, actual: Int)

object ChiSquareEntry {

  def fromDSEntry(entry: (Vector, Double, Double, Long)):
  ChiSquareEntry = {
    val (z, pNull, pAct, numZ) = entry
    val gen = (p: Double) => math.round(p.toFloat * numZ).toInt
    val nullHypo=gen(pNull)
    val actual=gen(pAct)
    ChiSquareEntry(nullHypo=nullHypo, actual=actual)
  }
}

trait WithChiInputs {
  val spark: SparkSession
  val ds: Dataset[ChiSquareEntry]
}

trait WithChiScore extends WithChiInputs {
  val chiScore: Double
}

trait WithNormlizedScore extends WithChiScore {
  val degreesOfFreedom: Int
  val normlizedScore: Double
}

object Statistics {

  def withChiInputs(s: SparkSession, d: Dataset[ChiSquareEntry]):
    WithChiInputs = {
    return new WithChiInputs
    { 
      val spark: SparkSession = s
      val ds: Dataset[ChiSquareEntry] = d 
    }
  }

  /**
   *  Calculate the chi^2 score from the given dataset.
   *  Return value: the score of the Chi^2 test.
   **/
  def withChiScore(inputs: WithChiInputs): WithChiScore = {
    import inputs.spark.implicits._

    val EPSILON = 0.0001
    val sumOfSDiff = inputs.ds
      .map(entry => {
        math.pow(entry.actual - entry.nullHypo + EPSILON, 2) /
        (EPSILON + entry.nullHypo)
      })
    val score = sumOfSDiff
      .groupBy()
      .sum()
      .as[Double]
      .first()

    new WithChiScore {
      val spark: SparkSession = inputs.spark
      val ds: Dataset[ChiSquareEntry] = inputs.ds
      val chiScore: Double = score
    }
  }

  def withNormlizedScore(inputs: WithChiScore, K: Int): WithNormlizedScore = {
    import inputs.spark.implicits._

    val numSamples = inputs.ds
      .groupBy()
      .sum("actual")
      .collect()(0)
      .getLong(0)
    assume(numSamples>0, () => s"number of samples is zero?")
    val k = math.max(1, K-1)
    val r = numSamples - 1

    println(s"EM k=${k}")
    println(s"EM r=${r}")
    return new WithNormlizedScore {
      val spark=inputs.spark
      val ds=inputs.ds
      val chiScore=inputs.chiScore
      val degreesOfFreedom=K
      val normlizedScore=math.sqrt(inputs.chiScore/math.min(k, r))
    }
  }

  def normlizeScore(spark: SparkSession)(
    ds: Dataset[ChiSquareEntry], degreesOfFreedom: Int, chiScore: Double):
  Double = {
    val numSamples = ds
      .groupBy()
      .sum("actual")
      .collect()(0)
      .getLong(0)
      assume(numSamples>0, () => s"number of samples is zero?")
      val k = math.max(1, degreesOfFreedom-1)
      val r = numSamples - 1

      println(s"EM k=${k}")
      println(s"EM r=${r}")
      math.sqrt(chiScore/math.min(k, r)) 
  }

  def pValue(score: Double, degreesOfFreedom: Int): Double = {
    val chi = new ChiSquaredDistribution(degreesOfFreedom)
    return chi.cumulativeProbability(score)
  }
}
