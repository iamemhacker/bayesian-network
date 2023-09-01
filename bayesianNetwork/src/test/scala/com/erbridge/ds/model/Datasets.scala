package com.erbridge.ds.network

import org.apache.spark.sql.{Dataset, SparkSession, functions => F}
import org.apache.spark.ml.feature.{StringIndexer, Bucketizer}
import scala.util.Random
import org.apache.commons.math3.distribution.NormalDistribution


object Datasets {
  def directIndepentValuesSet(spark: SparkSession): Dataset[Array[Int]]= {
    import spark.implicits._

    val r = 1 to 10
    r.map(x => r.map(y => Array(x, y))).flatten.toSeq.toDS
  }

  def invalidH0Set(spark: SparkSession): Dataset[ChiSquareEntry] = {
    import spark.implicits._

    val n = () => ((Random.nextInt % 100) + 100) % 100
    (1 to 100).map(_ => ChiSquareEntry(nullHypo=n(), actual=n()))
      .toSeq
      .toDS
  }

  def noisyValidH0Set(spark: SparkSession, noiseLevel: Double):
    Dataset[ChiSquareEntry] = {
    import spark.implicits._

    val norm = new NormalDistribution(0.0, noiseLevel)
    (1 to 100).map(x => ChiSquareEntry(nullHypo=x,
                                       actual=(x + norm.sample()).toInt))
      .toSeq
      .toDS
  }

  def gdpDataset(spark: SparkSession): Dataset[Array[Int]] = {
    import spark.implicits._

    val countryIndexer = new StringIndexer()
      .setInputCol("country")
      .setOutputCol("_country")
    val contIndexer = new StringIndexer()
      .setInputCol("continent")
      .setOutputCol("_continent")
    val langIndexer = new StringIndexer()
      .setInputCol("language")
      .setOutputCol("_language")
    val btizer = new Bucketizer()
      .setInputCol("gdp")
      .setOutputCol("_gdp")
      .setSplits(Array(0.0, 1000.0, 10000.0, 30000.0, Double.MaxValue))

    val ds = Seq(
      // Country | Continent | Spoken Language |  GDP per capita
      ("Burkina Faso",  "Africa",  "French", 825),
      ("Maldives",      "Africa",  "Dhivehi", 14000),
      ("Niger",         "Africa",  "French", 510),
      ("Guinea",        "Africa",  "French", 818),
      ("Singapore",     "Asia",    "English", 80000),
      ("Hong Kong",     "Asia",    "English", 70000),
      ("Iraq",          "Asia",    "Arabic", 7000),
      ("Cyprus",        "Europe",  "Greek", 30000),
      ("Lebanon",       "Asia",    "Arabic", 2785),
      ("Spain",         "Europe",  "Spanish", 29000),
      ("France",        "Europe",  "French", 45000),
      ("Korea",         "Asia",       "Korean", 35000),
      ("New Zealand",   "Oceania",    "English", 47200),
      ("Luxemburg",     "Europe",     "French", 135000),
      ("Ukraine",       "Europe",     "Ukranian", 4800),
      ("U.S",           "N. America", "English", 75000),
      ("China",         "Asia",       "Chinese", 13000),
      ("Israel",        "Asia",       "Hebrew", 50000),
      ("Norway",        "Europe",     "Norwegian", 97000),
      ("Kazakhstan",    "Europe",     "Russian", 10000),
      ("Belgium",       "Europe",     "French", 50000),
      ("Ireland",       "Europe",     "English", 101000),
      ("Chile",         "S. America", "Spanish", 15000),
      ("Barbados",      "S. America", "English", 18000),
    )
    .toDF("country", "continent", "language", "gdp")

    val transformers = Array(countryIndexer, contIndexer, langIndexer).
      map(idxer => idxer.fit(ds)) :+ btizer

    transformers
      .foldLeft(ds)((df, t) => t.transform(df))
      .select(F.array("_country", "_continent", "_language", "_gdp"))
      .as[Array[Double]]
      .map(arr => arr.map(x => x.toInt))
      .as[Array[Int]]
  }
}

