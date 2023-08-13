package com.agoda.ds.validation

import com.agoda.ds.types._
import com.agoda.ds.network.{Statistics}
import org.apache.spark.sql.{SparkSession, functions => F,  Dataset, DataFrame,
  Column}
import org.apache.spark.sql.expressions.Window

object Validation {

  val VALIDATION_SIZE = 1E4.toInt

  def predict(spark: SparkSession)(dsProbabilities: Dataset[LabeledEntry]):
    Dataset[ScoredLabeledEntry] = {
    import spark.implicits._

    val dropRandom = (ds: Dataset[LabeledEntry]) => {
      // Drop the random feature, at location 0.
      ds.map(e => e.copy(features=e.features.tail))
    }

    val coalesce = (ds: Dataset[LabeledEntry]) => {
      // Coalesce identical probability entries.
      ds
        .groupByKey(e => e.features)
        .reduceGroups((l, r) => {
          l.copy(probability=l.probability + r.probability)
        })
        .map(e => e._2)
    }

    dsProbabilities
      .transform(dropRandom)
      .transform(coalesce)
      .transform(df => calcScore(spark, df))
  }

  def calcScore(spark: SparkSession, ds: Dataset[LabeledEntry]):
    Dataset[ScoredLabeledEntry] = {
    import spark.implicits._

    val seperate = (e: LabeledEntry) => {
      (e.features.dropRight(1), e.features.last)
    }

    val brdcst = spark.sparkContext.broadcast(ds.limit(VALIDATION_SIZE).collect)
    ds.mapPartitions((entries: Iterator[LabeledEntry]) => {
      entries.map(e => {
        val (eF, eT) = seperate(e)
        val comp = brdcst.value.filter(cur => {
          val (cF, cT) = seperate(cur)
          eF.sameElements(cF) && cT != eT
        })
        val entry = LabeledEntry(features=e.features, e.probability)
        if (comp.isEmpty) {
          ScoredLabeledEntry(
            entry=entry,
            score=Double.PositiveInfinity)
        }
        else {
          val denum = comp.map(e => e.probability).reduce((l, r) => l + r)
          ScoredLabeledEntry(
            entry=entry,
            score=e.probability/denum)
        }
      })
    })
  }

  def summarize(spark: SparkSession)(
    dsPredicted: Dataset[ScoredLabeledEntry],
    dsLabled: Dataset[ComponentDiscrete]): DataFrame = {
    null
  }

}

