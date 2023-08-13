package com.agoda.ds.network

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.Tag
import org.scalatest.Assertions._
import org.apache.spark.sql.{Dataset, SparkSession, functions => F}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import scala.util.Random
import org.apache.commons.math3.distribution.NormalDistribution

object ChiSquare extends Tag("com.agoda.ChiSquareTest")

class ProbabilityTest extends AnyFlatSpec {

  implicit val spark = SparkSession
    .builder
    .master("local")
    .getOrCreate()

  private def toChiScheme(ds: Dataset[Array[Int]]): Dataset[ChiSquareEntry] = {
    import spark.implicits._
    
    ds.map(arr => ChiSquareEntry(nullHypo=arr(0), actual=arr(1)))
  }

  "Chi^2 tests" should "indentify dependent variables" taggedAs(ChiSquare) in {
    import spark.implicits._

    val ds = (1 to 100)
      .toSeq
      .map(x => ChiSquareEntry(nullHypo=x, actual=x))
      .toDS
    val res = Statistics.chiSquare(spark, 100*100)(ds)
    val pval = Statistics.pValue(res, 100*100)
    assert(pval < 1.0E-4)
  }

  "Chi^2 test" should "identify independency between variables" taggedAs(ChiSquare) in {
    import spark.implicits._

    val res = Statistics.chiSquare(spark, 100*100)(Datasets.invalidH0Set(spark))
    val pval = Statistics.pValue(res, 100*100)
    assert(pval > 0.95)
  }

  "Chi^2 test" should "be sensitive to noise levels" taggedAs(ChiSquare) in {
    import spark.implicits._

    val chi_2 = Statistics.chiSquare(spark, 100*100) _
    val (ds1, ds2) = (Datasets.noisyValidH0Set(spark, 1.0),
                      Datasets.noisyValidH0Set(spark, 3.0))
    val (stats1, stats2) = (chi_2(ds1), chi_2(ds2))
    val (pval1, pval2) = (Statistics.pValue(stats1, 100*100),
                          Statistics.pValue(stats2, 100*100))
    println(s"(${pval1}, ${pval2})")
    assert(pval1 < pval2)
  }

  val g = new Graph(nodes=List(Node(name="a", cardinality=100, id=0),
                               Node(name="b", cardinality=100, id=1)),
                edges=List.empty[Edge])

  val g2 = new Graph(
      nodes=List(
        Node(id=0, name="a", cardinality=100),
        Node(id=1, name="b", cardinality=100),
        Node(id=2, name="c", cardinality=100)),
      edges=List.empty[Edge])

  "Empty cut set" should "separate X from Y, if  X |- Y" in {
    import spark.implicits._

    val ds = Datasets.directIndepentValuesSet(spark)
    // Building a dataset, for which every x in X has equal probability to
    // appear with every y in Y.
    val isSeparationSet = Probability.isSeparationSet(
      spark, ds, Array(100, 100, 100))(0, Seq(1), Seq.empty[Int])
    assert(isSeparationSet)
  }
  
  //"Empty cut set" should "separates X from Y if X _|_ Y"  in {
    //import spark.implicits._

    //val ds = (1 to 100)
      //.map(x => x%5)
      //.map(x => Array(x,  x))
      //.toSeq
      //.toDS
    
    //assertResult(false) {
      //Probability.isSeparationSet(spark, ds, g)(0, 1, Seq.empty[Int])
    //}
  //}

  //"Cut set" should "Z that separates X and Y" in {
    //import spark.implicits._

    //val ds = Seq(
        //// X a random number, Y parity of X, Z is Y * 3.
        //// P(x, y | z) = P(x | z) * P(y | z).
        //Array(10, 0, 0),
        //Array(7, 1, 3),
        //Array(4, 0, 0),
        //Array(5, 1, 3),
        //Array(6, 0, 0),
        //Array(19, 1, 3)
      //).toDS
    //val indie = Probability.isSeparationSet(spark, ds, g2) _
    //assert(indie(0, 1, List(2)))
  //}

  //"Cut set" should "not be created here," + 
  //"since Z does not properly separates X and Y" in {
    //import spark.implicits._

    //val ds = Seq(
      //Array(10, 0, 1),
      //Array(4, 0, 1),
      //Array(6, 0, 1),
      //Array(19, 1, 1),
      //Array(10, 0, 2),
      //Array(6, 0, 2)).toDS

    //val indie = Probability.isSeparationSet(spark, ds, g2) _

    //// When x is on column '0', y in on '1', and 'z' on 2
    // z 'doesn't add' any information about x, therefore
    // p(x,y | z) != p(x|z)p(y|z)
    //assert(!indie(0, 1, List(2)))

    //// When x is on column '0', y in on '2', and 'z' on 1
    //// z indicates the parity of x. 'y' is directly affected from the value of
    //// 'z', thus p(x,y | z) = p(x|z)p(y|z).
    //assert(indie(0, 2, List(1)))
  //}

}
