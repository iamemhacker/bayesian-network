package com.erbridge.ds.network

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.Tag
import org.scalatest.Assertions._
import org.apache.spark.sql.{Dataset, SparkSession, functions => F}
import com.erbridge.ds.types.ComponentDiscrete


class NetworkBuilderTest extends AnyFlatSpec {
  implicit val spark = SparkSession
    .builder
    .master("local")
    .getOrCreate()

  val builder = NetworkBuilder.build(spark) _

  it should "idenify the minimal cutset between GDP and country" in {
    import spark.implicits._

    val dsGDP = Datasets.gdpDataset(spark)
      .map(arr => ComponentDiscrete(features=arr.dropRight(1), target=arr.last))
    val cardinalities = Array(24, 5, 13, 5)
    val graph = builder(
      Array("country", "continent", "language", "GDP"),
      cardinalities,
      dsGDP)
    NetworkBuilder.save(spark)(graph, "_graph-gdp/")

    val edges = graph.edges
    assert(edges.length == 5)
    assert(edges.exists(e => e == Edge(0, 1)))
    assert(edges.exists(e => e == Edge(0, 2)))
    assert(edges.exists(e => e == Edge(1, 2)))
    assert(edges.exists(e => e == Edge(1, 3)))
    assert(edges.exists(e => e == Edge(2, 3)))
    assert(!edges.exists(e => e == Edge(0, 3)))

  }

}
