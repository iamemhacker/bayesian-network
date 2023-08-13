package com.agoda.ds.network

case class Node(
  // ID of the node (index within the nodes array).
  id: Int,
  // Meaningful name for the node.
  name: String,
  // Number of possible values.
  cardinality: Int
)

case class Edge(src: Int, dst: Int, score: Double)


class Graph(val nodes: List[Node], val edges: List[Edge]) extends Serializable {

  def numNodes(): Int = nodes.length

  def nodeAt(idx: Int): Node = {
    require(idx >= 0 && idx<nodes.length)
    nodes(idx)
  }

  def edgeExists(e: Edge): Boolean = {
    edges.exists(e_ => e == e_)
  }

  def removeEdge(edge: Edge): Graph = {
    new Graph(nodes, edges=edges.filter(e => e != edge))
  }

  def parents(node: Node): List[Node] = {
    edges.filter(e => e.dst == node.id).map(e => nodeAt(e.src))
  }

  def scores(node: Node): List[Double] = {
    edges.filter(e => e.dst == node.id).map(e => e.score)
  }

  def entryLevel(node: Node): Int = {
    edges.foldLeft(0)((l: Int, e: Edge) => {
      if (e.dst == node.id) {
        1 + l
      } else {
        l
      }
    })
  }

  override def toString(): String = {
    s"""nodes: [${nodes.mkString(" ")}]
    edges: [${edges.mkString(", ")}]
    """
  }
}
