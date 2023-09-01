package com.erbridge.ds.network

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.Assertions._
import org.apache.spark.sql.{Dataset, SparkSession, functions => F}
import com.erbridge.ds.types.ComponentDiscrete


class NetworkQueryTest extends AnyFlatSpec {
  implicit val spark = SparkSession
    .builder
    .master("local")
    .getOrCreate()

  it should "Build a network." in {
    import spark.implicits._

    val ds = Seq(
      ComponentDiscrete(features=Array(0), target=1),
      ComponentDiscrete(features=Array(0), target=1),
      ComponentDiscrete(features=Array(0), target=2),
      ComponentDiscrete(features=Array(1), target=1),
      ComponentDiscrete(features=Array(1), target=2),
      ComponentDiscrete(features=Array(1), target=2),
      ComponentDiscrete(features=Array(2), target=0),
      ComponentDiscrete(features=Array(2), target=0),
      ComponentDiscrete(features=Array(2), target=0),
      ComponentDiscrete(features=Array(2), target=1),
      ComponentDiscrete(features=Array(3), target=1),
      ComponentDiscrete(features=Array(3), target=2),
      ComponentDiscrete(features=Array(3), target=3),
      ComponentDiscrete(features=Array(4), target=4),
      ComponentDiscrete(features=Array(4), target=4),
      ComponentDiscrete(features=Array(4), target=4),
      ComponentDiscrete(features=Array(4), target=4),
      ).toDS
    
    val cardinalities = Array(5, 5)
    val model = NetworkQuery.fit(
      spark=spark,
      cache=Option.empty[String],
      names=Array("n1", "n2"),
      cardinalities=cardinalities)(data=ds)
      .collect

    assert(model(0).parentIDs.isEmpty)
    assert(model(0).nodeId == 0)
    assert(model(1).parentIDs.sameElements(Array(0)))
    assert(model(1).nodeId == 1)
  }

  it should "produce accurate queries over the dataset" in {
    import spark.implicits._

    val dsGDP = Datasets.gdpDataset(spark)
      .map(arr => ComponentDiscrete(features=arr.dropRight(1), target=arr.last))
    val cardinalities = Array(24, 5, 13, 5)
    val model = NetworkQuery.fit(
      spark=spark,
      cache=Some("_graph-gdp/"),
      names=Array("country", "continent", "language", "GDP"),
      cardinalities=cardinalities)(data=dsGDP)
    
    val predictions = NetworkQuery.transform(spark, model)(dsGDP).collect
    val p = predictions
      .filter(e => e.features.sameElements(Array(0, 3, 0, 2)))
      .head
      .probability
    assert(math.abs(p - 1.0/predictions.length) < 0.00001)
  }

}
