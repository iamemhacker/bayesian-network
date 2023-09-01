package com.erbridge.ds.validation

import com.erbridge.ds.types.{ComponentDiscrete, LabeledEntry}
import org.apache.spark.sql.{SparkSession, Dataset}


object Oracle {
  def calcProbabilities(spark: SparkSession,
                        dsPredict: Dataset[ComponentDiscrete]):
  Dataset[LabeledEntry] = {
    import spark.implicits._
    val N = dsPredict.count

    dsPredict
      .map(entry => entry.features :+ entry.target)
      .groupByKey(x => x)
      .count
      .as[(Array[Int], Long)]
      .map(p => LabeledEntry(features=p._1, p._2.toDouble/N))
  }
}

