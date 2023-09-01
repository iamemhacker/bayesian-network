package com.erbridge.ds

import org.apache.spark.sql.{ Dataset, SparkSession, Column }

final class ChainingOps[A](private val v: A) extends AnyVal {
  def pipe[B](f: A => B): B = f(v)
}

object Utils {

  def bind[U, V](spark: SparkSession,
    f: (SparkSession, Dataset[U]) => Dataset[V]): (Dataset[U]) => Dataset[V] = {
    import spark.implicits._

    return (df: Dataset[U]) => f(spark, df)
  }

  def bind2[T, U, V](spark: SparkSession,
    ctx: T,
    f: (SparkSession, T, Dataset[U]) => Dataset[V]):
    (Dataset[U]) => Dataset[V] = {
    import spark.implicits._

    return (df: Dataset[U]) => f(spark, ctx, df)
  }

  trait ChainingSyntax {
    implicit final def scalaUtilChainingOps[A](a: A) : ChainingOps[A] = {
      new ChainingOps(a)
    }
  }

  object chaining extends ChainingSyntax

  def toHumanReadable(d: Double): String = {
    val suffixes = Seq("", "K", "M", "G", "T")
    val idx = math.max(0, math.log10(d).toInt / 3)
    s"${d/math.pow(1000, idx)}${suffixes(idx)}"
  }

}
