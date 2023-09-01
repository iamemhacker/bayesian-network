package com.erbridge.ds.types

import org.apache.spark.sql.{Column, DataFrame, Dataset, functions => F,
  SparkSession}
import org.apache.spark.sql.expressions.Window
import java.time.LocalDate

import com.erbridge.ds.{Ordinal, Cardinality}

object DtUtil {
  val strDTFormat = "yyyyMMdd"

  def dayDiff(beg: Column, end: Column): Column = {
    val (dt1, dt2) = (
      F.to_date(beg, strDTFormat),
      F.to_date(end, strDTFormat))
    F.datediff(dt2, dt1)
  }
}

object TypeUtils {
  def getFieldOrder[T](v: T): Array[(String, Int)] = {
    val clsz = classOf[Ordinal]
    val params = v
      .getClass()
      .getConstructors()(0)
      .getParameters()
      .filter(p => p.isAnnotationPresent(clsz))
    val names = params.map(p => p.getName())
    val ordinals = params
      .map(p => p.getAnnotationsByType(clsz))
      .flatten
      .map(o => o.value())

    names.zip(ordinals)
  }

  def getFieldCardinality[T](v: T): Array[(Int, Int)] = {
    val clsz = classOf[Cardinality]
    val params = v
      .getClass()
      .getConstructors()(0)
      .getParameters()
      .filter(p => p.isAnnotationPresent(clsz))
    val ordinals = getFieldOrder(v).map(p => p._2)
    val cardinalities = params
      .map(p => p.getAnnotationsByType(clsz))
      .flatten
      .map(o => o.value())
    ordinals.zip(cardinalities)
  }
}

case class ComponentDiscrete(
  features: Array[Int],
  target: Int
)

case class LabeledEntry(
  features: Array[Int],
  probability: Double
)

case class ScoredLabeledEntry(
  entry: LabeledEntry,
  score: Double
)
