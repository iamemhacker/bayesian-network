package com.erbridge.ds.features

import org.apache.spark.sql.{Column, functions => F, Dataset, SparkSession,
  DataFrame}
import com.erbridge.ds.types.{
  ComponentDiscrete,
  DtUtil,
  TypeUtils
}
import org.apache.spark.ml.feature.Bucketizer


object Conversions {

  // Define conversions from source column to discrete columns (of type Int).
  private def encodeColumn1(df: DataFrame):  DataFrame = {
    df
  }

  def toCanonicalForm[T](
    spark: SparkSession,
    ds: Dataset[T]): Dataset[ComponentDiscrete] = {
      import spark.implicits._

      //
      // Columns encoding...
      //
      val dfDiscrits = ds
        .toDF
        .transform(encodeColumn1)
        //.transform(encodeColumn2)
        //.transform(encodeColumn3)

      //
      // Schema conversion.
      //
      //val discreteCols = TypeUtils
        //.getFieldOrder(MatchingFeatures())
        //.sortBy(p => p._2)
        //.map(p => p._1)
        //.map(c => F.col(prefix(c)).cast("int"))
        //

      dfDiscrits.as[ComponentDiscrete]
  }
}
