package com.erbridge.ds.network

import org.apache.spark.sql.{Dataset, SparkSession, functions => F, DataFrame,
  Column}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import com.erbridge.ds.types.{ComponentDiscrete, LabeledEntry}
import org.apache.spark.ml.linalg.{Vectors, Vector, Matrix, Matrices}
import org.apache.spark.broadcast.Broadcast
import com.erbridge.ds.Utils


case class ModelEntry(
  parentIDs: Array[Int],
  nodeId: Int)

object ModelEntry {
  def name(me: ModelEntry): String = {
    if (me.parentIDs.isEmpty)
      s"P(${me.nodeId})"
    else
      s"P(${me.nodeId}|${me.parentIDs.mkString(",")})"
  }
}

object NetworkQuery {

  def loadGraph(spark: SparkSession,
                cache: Option[String],
                names: Array[String],
                cardinalities: Array[Int],
                data: Dataset[ComponentDiscrete]): Graph = {
    if (cache.isEmpty) {
      NetworkBuilder.build(spark)(names, cardinalities, data)
    } else {
      NetworkBuilder.load(spark)(cache.get)
    }
  }

  def fit(spark: SparkSession,
          names: Array[String],
          cardinalities: Array[Int],
          cache: Option[String])(
          data: Dataset[ComponentDiscrete]): Dataset[ModelEntry] = {
    import spark.implicits._

    val graph = loadGraph(spark, cache, names, cardinalities, data)
    graph.nodes
      .map(n => ModelEntry(nodeId=n.id,
                           parentIDs=graph.parents(n).map(n => n.id).toArray))
      .toSeq
      .toDS
  }

  def transform(spark: SparkSession, dsModel: Dataset[ModelEntry])(
                dsData: Dataset[ComponentDiscrete]): Dataset[LabeledEntry] = {
    import spark.implicits._

    val idToName = (id: Int) => s"f${id}"
    val graph = dsModel.collect
    val count = (arr: Array[Int]) => {
      val w = Window.partitionBy(arr.map(i => F.col(idToName(i+1))):_*)
      F.count("*").over(w).as(s"#[${arr.mkString(",")}]")
    }
    val countColumns = graph.map((me: ModelEntry) => {
      (count(me.parentIDs :+ me.nodeId) / count(me.parentIDs))
      .as(ModelEntry.name(me))
    })
    val jointP = (pcols: Array[Column]) => {
      pcols.reduce((l, r) => l*r).as("p")
    }

    // Flatten the data, from a single column array, to multiple scalar columns.
    val N = dsModel.count.toInt
    val feildCols = (1 to N).map(id => {
      val name = idToName(id)
      F.element_at(F.col("value"), id).as(name)
    })
    val namedCols = (1 to N).map(i => F.col(idToName(i)))

    val dataFlatten = dsData
      .map(e => e.features :+ e.target)
      .select(feildCols:_*)

    val dsCounts = dataFlatten.select((namedCols ++
                                       countColumns
                                       :+ jointP(countColumns)):_*)
    dsCounts.select(F.array(namedCols:_*).as("features"),
                    F.col("p").as("probability"))
            .distinct
            .as[LabeledEntry]
  }

}
