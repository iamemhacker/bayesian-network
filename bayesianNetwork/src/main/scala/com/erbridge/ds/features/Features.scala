package com.erbridge.ds.features

import org.apache.spark.sql.{
  Column,
  functions => F,
  Dataset,
  SparkSession,
  DataFrame}
import org.apache.spark.sql.expressions.Window
import com.erbridge.ds.types.{
  ComponentDiscrete,
  DtUtil,
  TypeUtils
}
import org.apache.spark.ml.feature.{Bucketizer, StringIndexer}


object Conversions {

  val featureNames = Array(
    "PTS",
    "FGM",
    "FGA",
    "FG%",
    "3PM",
    "3PA",
    "3P%",
    "FTM",
    "FTA",
    "FT%",
    "OREB",
    "DREB",
    "REB",
    "AST",
    "STL",
    "BLK",
    "TOV",
    "PF", 
    "EFF",
    "AST/TOV",
    "Age",
    "Birth_Place",
    "Collage",
    "Experience",
    "Height",
    "Pos",
    "Team",
    "Weight",
    "BMI")

  val featureCardinalities = (ds: Dataset[ComponentDiscrete]) => {
    val idxCol = (idx: Int) => F.element_at(F.col("features"), idx)
    val cols = (1 to featureNames.length)
      .map(idx => F.count_distinct(idxCol(idx)).cast("int").as(s"c${idx}"))
    val row = ds.select(cols:_*).first
    Array((0 until row.length).map(idx => row.getInt(idx)):_*)
  }

  def toCanonicalForm[T] (
    spark: SparkSession,
    ds: Dataset[T]): Dataset[ComponentDiscrete] = {
      import spark.implicits._

      val trgtCol = "MIN"

      val rename = (name: String) => s"${name}_enc"

      def intCol(df: DataFrame, inColName: String, outColName: String, numBuckets: Int=10) = {
        val win = Window.partitionBy()
        val percentiles = (0 to numBuckets).map(i => i/numBuckets.toDouble).toArray
        val dfCasted = df.withColumn("_", F.col(inColName).cast("double"))

        // Calculating the 'splits' array, which is the percentile values of the column.
        val splits = dfCasted
          .select(F.percentile_approx(F.col("_"), F.lit(percentiles), F.lit(1000)))
          .as[Array[Double]]
          .first
          .toSet
          .toArray
          .sorted

        new Bucketizer()
          .setInputCol("_")
          .setOutputCol(outColName)
          .setSplits(splits)
          .transform(dfCasted)
          .drop("_")
          .select((df.columns.map(F.col) :+ F.col(outColName).cast("int").as(outColName)):_*)
      }

      def strCol[T] = (df: DataFrame, inColName: String, outColName: String) => {
        // Replace Null value with N/A to avoid crush from the StringIndexer.
        val dfFilled = df.na.fill(value="NA", cols=Seq(inColName))
        new StringIndexer()
          .setInputCol(inColName)
          .setOutputCol(outColName)
          .fit(dfFilled)
          .transform(dfFilled)
          .select((df.columns.map(F.col) :+ F.col(outColName).cast("int").as(outColName)):_*)
      }

      val dtCol = (df: DataFrame, inColName: String, outColName: String) => {
        df.withColumn(outColName, F.to_date(F.col(inColName), "MMMM d, yyyy"))
      }

      //
      // Columns encoding...
      //
      val dfEncoded = ds
        .toDF
        .transform(df => intCol(df, "PTS", rename("PTS")))
        .transform(df => intCol(df, "FGM", rename("FGM")))
        .transform(df => intCol(df, "FGA", rename("FGA")))
        .transform(df => intCol(df, "FG%", rename("FG%")))
        .transform(df => intCol(df, "3PM", rename("3PM")))
        .transform(df => intCol(df, "3PA", rename("3PA")))
        .transform(df => intCol(df, "3P%", rename("3P%")))
        .transform(df => intCol(df, "FTM", rename("FTM")))
        .transform(df => intCol(df, "FTA", rename("FTA")))
        .transform(df => intCol(df, "FT%", rename("FT%")))
        .transform(df => intCol(df, "OREB", rename("OREB")))
        .transform(df => intCol(df, "DREB", rename("DREB")))
        .transform(df => intCol(df, "REB", rename("REB")))
        .transform(df => intCol(df, "AST", rename("AST")))
        .transform(df => intCol(df, "STL", rename("STL")))
        .transform(df => intCol(df, "BLK", rename("BLK")))
        .transform(df => intCol(df, "TOV", rename("TOV")))
        .transform(df => intCol(df, "PF", rename("PF")))
        .transform(df => intCol(df, "EFF", rename("EFF")))
        .transform(df => intCol(df, "AST/TOV", rename("AST/TOV")))
        .transform(df => intCol(df, "Age", rename("Age")))
        .transform(df => strCol(df, "Collage", rename("Collage")))
        .transform(df => intCol(df, "Experience", rename("Experience")))
        .transform(df => intCol(df, "Height", rename("Height")))
        .transform(df => strCol(df, "Pos", rename("Pos")))
        .transform(df => strCol(df, "Team", rename("Team")))
        .transform(df => intCol(df, "Weight", rename("Weight")))
        .transform(df => intCol(df, "BMI", rename("BMI")))

      val encodedCols = dfEncoded.columns.filter(n => n.endsWith("_enc"))

      
      //
      // Schema conversion.
      //
      //val dfEncoded = TypeUtils
        //.getFieldOrder(MatchingFeatures())
        //.sortBy(p => p._2)
        //.map(p => p._1)
        //.map(c => F.col(prefix(c)).cast("int"))

      dfEncoded
        .select(F.array(encodedCols.map(F.col):_*).as("features"),
                F.col(trgtCol).cast("int").as("target"))
        .as[ComponentDiscrete]
  }
}
