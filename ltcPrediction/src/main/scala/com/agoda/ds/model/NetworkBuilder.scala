package com.agoda.ds.network

import com.agoda.ds.types.{ComponentDiscrete}
import org.apache.spark.sql.{Dataset, SparkSession, functions=>F}

object NetworkBuilder {

  val DEBUG = true

  val RATIO_THRESHOLD = 0.0

  def build(spark: SparkSession)(names: Array[String],
                                 cardinalities: Array[Int],
                                 ds: Dataset[ComponentDiscrete]): Graph = {
    import spark.implicits._

    println(s"EM cardinalities=${cardinalities.mkString(", ")}")
    require(names.length == cardinalities.length)
    require(names.length > 1)
    require((ds.first().features.length + 1) == names.length)
    
    val nodes = (0 until names.length)
      .map(idx => new Node(id=idx,
                           name=names(idx),
                           cardinality=cardinalities(idx)))
      .toList

    val numNodes = nodes.length
    val (node0, node1) = (nodes(0), nodes(1))
    val dsFlat: Dataset[Array[Int]] = ds.map(e => e.features :+ e.target)

    val edgeScore = Probability.isSeparationSet(spark, dsFlat, cardinalities) _
    println(s"EM testing for Edge(0, 1)")
    val score = edgeScore(1, Seq.empty[Int], Seq(0))
    val initEdges = if (score < RATIO_THRESHOLD) {
      println("EM Edge(0, 1) is not placed")
      Seq.empty[Edge]
    } else {
      println("EM Edge(0, 1) is placed")
      Seq(Edge(src=0, dst=1, score=score))
    }

    val edges = (2 until numNodes).foldLeft(initEdges)(
      (e: Seq[Edge], dst: Int) => {
        // TODO: solve that BS.
        var scores = Seq.empty[Double]
        val pset = (0 until dst).foldLeft(Seq((0 until dst):_*))(
          (set: Seq[Int], toDrop: Int) => {
            val reducedSet = set.filter(id => id != toDrop)
            println(s"EM testing for Edge(${toDrop}, ${dst})")
            val score = edgeScore(dst, reducedSet, set)
            if (score < RATIO_THRESHOLD) {
              println(s"EM Edge(${toDrop}, ${dst}) is not placed")
              reducedSet
            } else {
              scores = scores :+ score
              println(s"EM Edge(${toDrop}, ${dst}) is placed")
              set 
            }
          })
        e ++ pset
          .zipWithIndex
          .map(p => {
            val (src, idx) = p
            Edge(src=src, dst=dst, score=scores(idx))
          })
      })
    new Graph(nodes, edges.toList)
  }

  def save(spark: SparkSession)(graph: Graph, path: String): Unit = {
    import spark.implicits._

    val ds = graph.nodes.map(node => {
      (node.id,
       node.cardinality,
       node.name,
       graph.parents(node).map(n => n.id).toArray,
       graph.scores(node).toArray)
    }).toSeq.toDS
    
    ds.coalesce(1).write.mode("overwrite").json(path)
  }

  def load(spark: SparkSession)(path: String): Graph = {
    import spark.implicits._

    val cols = Seq(F.col("_1"),
                   F.col("_2").cast("int"),
                   F.col("_3"),
                   F.col("_4"),
                   F.col("_5"))
    val ds = spark.read.json(path)
      .select(cols:_*)
      .as[(Long, Int, String, Array[Long], Array[Double])]
    val v =  ds.map(e => Node(id=e._1.toInt, cardinality=e._2, name=e._3))
               .collect.toList
    val scores = ds.map(e => e._5).collect.toList
    val e = ds
      .map(e => e._4
                 .zipWithIndex
                 .map(p => {
                   val (src, idx) = p
                   Edge(src=src.toInt, dst=e._1.toInt, score=e._5(idx))
                 }))
      .collect
      .toList
      .flatten
    new Graph(nodes=v, edges=e)
  }
}
