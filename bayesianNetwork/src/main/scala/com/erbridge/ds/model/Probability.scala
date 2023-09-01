package com.erbridge.ds.network

import com.erbridge.ds.Utils
import org.apache.spark.sql.{Dataset, SparkSession, functions => F, Column,
  DataFrame }
import org.apache.spark.sql.expressions.Window
import Utils.chaining._

case class ProbabilityEntry(observ: Array[Int], cond: Array[Int])

object Probability {
  val P_THRESHOLD = 0.02

  val OUT_LIARS_P = 0.05

  val DEBUG_REMOTE = true
  val DEBUG_LOCAL = false

  def countBySet(set: Seq[Int], xIdx: Int): Column  = {
    val N = 100
    val setCols = set.map(i => F.col(idxToName(i)))
    val winEnum = Window.partitionBy((setCols :+ F.col(idxToName(xIdx))):_*)
    val winDenum = Window.partitionBy(setCols:_*)

    F.round((F.lit(N) *
     (F.count("*").over(winEnum)/
      F.count("*").over(winDenum)))).cast("int")
  }

  val idxToName = (idx: Int) => s"f${idx}"

  val toCol = (idx: Int) => F.element_at(
      F.col("value"), idx+1).as(idxToName(idx))

  def flatten(ds: Dataset[Array[Int]]): DataFrame = {
    val dim = ds.first.length
    val fcols = (0 until dim).map(toCol)

    ds.select(fcols:_*)
  }

  /**
   * @description: Generates the chi-square entries dataset for the bosrved data
   * (i.e, P(x|set1),  P(x|set2)).
   **/
  def genChiForObserved(spark: SparkSession, ds: Dataset[Array[Int]])(
    xIdx: Int, set1: Seq[Int], set2: Seq[Int]): Dataset[ChiSquareEntry] = {
    import spark.implicits._

    println(s"set1: ${set1.mkString(",")}")
    println(s"set2: ${set2.mkString(",")}")
    val dsFlat = ds
      .transform(flatten)
      .select((set2 :+ xIdx).map(i => F.col(idxToName(i))):_*)

    dsFlat
      .withColumn("nullHypo", countBySet(set1, xIdx))
      .withColumn("actual", countBySet(set2, xIdx))
      .distinct
      .as[ChiSquareEntry]
  }

  /**
   * @description Generates the chi-square entries for the theoretical set.
   * Explanation:
   * We would like to test whether P(x, set1| set2) =? P(x| set2) P(set1| set2).
   * We need to take *all* the possible combinations in Y \time Z.
   * Thus, it is not enough to consider all the Y's and Z's that appeared together.
   **/
  def genChiForMissing(spark: SparkSession, ds: Dataset[Array[Int]])(
    xIdx: Int, set1: Seq[Int], set2: Seq[Int],
    cardinalities: Array[Int]): Dataset[ChiSquareEntry] = {
    import spark.implicits._

    val set2Cols = (set2 :+ xIdx).map(idxToName)
    val set1Cols = (set1 :+ xIdx).map(idxToName)

    val dfActual = ds
      .transform(flatten)
      .select(set2Cols.map(colName => F.col(colName)):_*)

    // The Omega set. (all possible assignments to the random variables)
    // in the containing set (set2).
    val dfSuperset = (set2 :+ xIdx)
      .map(idx => (0 until cardinalities(idx)).toDF(idxToName(idx)))
      .reduce((l, r) => l.crossJoin(r))

    val set2Cond = (dfl: DataFrame, dfr: DataFrame) => {
      set2Cols
        .map(colName => dfl(colName) === dfr(colName))
        .reduce((l, r) => l && r)
    }
    
    // All the rows that appears in the superset, but not in the actual data will be added
    // with '0' appearances.
    val dfMissing = dfSuperset
      // Filter-in the missing set2 assignments in dsActual.
      .joinWith(other=dfActual,
                condition=set2Cond(dfSuperset, dfActual),
                joinType="left_outer")
      .filter(p => p._2.isNullAt(0))
      .select(set2Cols.map(colName => F.col(s"_1.${colName}")):_*)

    println(s"EM: the count of dsMissing is ${dfMissing.count}")

    val set1Cond = (dfl: DataFrame, dfr: DataFrame) => {
      set1Cols
        .map(colName => dfl(colName) === dfr(colName))
        .reduce((l, r) => l && r)
    }

    // Filter-out the missing set1 assignments in dsActual,
    // so we end up with all the viable assignments.
    val dsValidS1 = dfActual.select(set1Cols.map(F.col):_*).distinct

    // Holds entries, for which: P(x|set1) > 0, and P(x|set2) = 0.
    val dsPosH0ZH1 = dfMissing
      .joinWith(other=dsValidS1, condition=set1Cond(dfMissing, dsValidS1))
      .select(set2Cols.map(colName => F.col(s"_1.${colName}")):_*)
    
    println(s"EM: the count of dsPosH0ZH1 is ${dsPosH0ZH1.count}")

    var dfWH0 = dfActual
      .withColumn("nullHypo", countBySet(set1, xIdx))
    dfWH0
      .joinWith(other=dsPosH0ZH1, condition=set2Cond(dfWH0, dsPosH0ZH1))
      .select((set2Cols :+ "nullHypo")
              .map(colName => F.col(s"_1.${colName}")):_*)
      .distinct
      .withColumn("actual", F.lit(0))
      .as[ChiSquareEntry]
  }

  def removeOutliers(spark: SparkSession, ds: Dataset[ChiSquareEntry]):
    Dataset[ChiSquareEntry] = {
    import spark.implicits._

    val win = Window
      .partitionBy()
      .orderBy(F.desc("diff"))
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val diffCol = F.abs( F.col("nullHypo") - F.col("actual"))
    val numRowsToDrop = (OUT_LIARS_P * ds.count).toInt

    println(s"EM Droping ${numRowsToDrop} rows.")
    ds.withColumn("diff", diffCol)
      .withColumn("rank", F.count("*").over(win))
      .filter(F.col("rank") > numRowsToDrop)
      .drop("diff", "rank")
      .as[ChiSquareEntry]
  }

  /** 
   * @description returns the \chi^2 for \Chi(P(x|set1), P(x|set2)).
   **/
  def isSeparationSet(spark: SparkSession,
                      ds: Dataset[Array[Int]],
                      cardinalities: Array[Int])(
                      xIdx: Int,
                      set1: Seq[Int],
                      set2: Seq[Int]): Double = {
    import spark.implicits._

    // set1 needs to be contained by s2.
    require(set1.intersect(set2).sameElements(set1))
    require(cardinalities.foldLeft(true)((f, x) => f && x > 0))

    println(s"""EM testing equivalence for
      | I[${xIdx}, (${set1.mkString(", ")}) (${set2.mkString(", ")})]"""
      .stripMargin('|').replaceAll("\n", " "))

    val dsObserved = genChiForObserved(spark, ds)(xIdx, set1, set2)
    val dsMissing = genChiForMissing(spark, ds)(xIdx, set1, set2, cardinalities)
    val dsChiInput = dsObserved.union(dsMissing)

    val s1 = if (set1.isEmpty) "E" else set1.mkString("-")
    val s2 = if (set2.isEmpty) "E" else set2.mkString("-")
    if (DEBUG_REMOTE) {
      dsChiInput
        .write
        .mode("overwrite")
        .parquet(s"/user/AGODA+emalul/out/x=${xIdx}/set1=${s1}/set2=${s2}/")
    }

    if (DEBUG_LOCAL) {
      dsChiInput
        .write
        .mode("overwrite")
        .parquet(s"/tmp/x=${xIdx}/set1=${s1}/set2=${s2}/")
    }

    // Calculate the edge score.
    val degreesOfFreedom = (set2 :+ xIdx)
      .map(idx => cardinalities(idx))
      .reduce((x, y) => x * y)
    val results = Statistics.withChiInputs(spark, dsChiInput)
      .pipe(Statistics.withChiScore)
      .pipe(s => Statistics.withNormlizedScore(s, degreesOfFreedom))

    println(s"EM chi score=${results.chiScore}")
    println(s"EM edge score is: ${results.normlizedScore}")

    return results.normlizedScore
  }

}
