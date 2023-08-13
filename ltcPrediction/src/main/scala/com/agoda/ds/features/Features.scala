package com.agoda.ds.features

import org.apache.spark.sql.{Column, functions => F, Dataset, SparkSession,
  DataFrame}
import org.apache.spark.sql.expressions.Window
import com.agoda.ds.types.{
  MatchingFeatures,
  ComponentDiscrete,
  DtUtil,
  TypeUtils
}
import org.apache.spark.ml.feature.Bucketizer


object Conversions {

  /**
   * Column selection for the pricing component.
   **/
  def pricingColumns(): Seq[Column] = {
    Seq (
      // Hotel related features.
      // TODO: Remove.
      (F.rand() * F.lit(2)).cast("int").as("randomNoise"),
      F.col("destinationCode"),
      F.col("current_star_rating").as("starRating"),
      // Modifiers.
      F.col("country").as("originCode"),
      F.col("device_type").as("deviceType"),
      F.col("length_of_stay").cast("int").as("lengthOfStay"),
      F.col("date_type").as("defaultDate"),
      F.col("check_in_date").as("checkinDate"),
      F.col("ltc").cast("int").as("ltc"))
  }

  /**
   * Adds a destination column for the booking.
   */ 
  def hotelTraits(dfHPA: DataFrame, dfHotels: DataFrame):
    DataFrame = {
      val lcols = dfHPA.columns.map(F.col)
      val destCols = Seq(
        F.col("country_name").as("destinationCode"),
        F.col("current_star_rating")
      )

      dfHPA.join(
        right=dfHotels,
        usingColumns=Seq("hotel_id"),
        joinType="left")
        .select((lcols ++ destCols):_*)
  }

  def withLTC(dfHpa: DataFrame): DataFrame = {
    // Calculate Lifetime-contribution as:
    // booking_margin + dltv_aggresive - total_variable_cost - cost.
    val calcLTC = () => {
      val winBen = Window.partitionBy("hotel_id")
      val ltmColumns = List(
        "booking_margin_prediction",
        "dltv_aggressive",
        "total_variable_cost",
        "billing_cost_usd")
      val colWeights = List(1, 1, -1, -1)
      val ltmpcColumns = ltmColumns.zip(colWeights)
      val sumCol = ltmpcColumns
        .map(p => F.col(p._1) * F.lit(p._2))
        .reduce((x, y) => x + y)
      F.sum(sumCol).over(winBen).as("ltc")
    }

    return dfHpa.select((dfHpa.columns.map(F.col) :+ calcLTC()):_*)
  }

  def discreteFeature[T](ds: Dataset[T], colName: String): DataFrame = {
    val win = Window
      .partitionBy()
      .rowsBetween(Window.unboundedPreceding, 0)

    val dfFeatureToBucket = ds.select(colName)
      .distinct
      .withColumn(s"d_${colName}", F.count("*").over(win))

    ds.join(
      right=dfFeatureToBucket,
      usingColumns=Seq(colName))
      .toDF
  }
  
  val prefix = (s: String) => s"d_${s}"

  val deprefix = (s: String) => s.drop(2)

  val NUM_LTC_BUCKETS = 3

  def percentile[T](ds: Dataset[T], colName: String, numPercentiles: Int=5):
    DataFrame = {
    val placeHolder = "_"
    val win = Window.partitionBy().orderBy(colName)
    val perCol = ((F.col(placeHolder) * 100) /
                   F.lit(numPercentiles)).cast("int")
    val capBy9 = (col: Column) => F.when(col === F.lit(numPercentiles),
                                         F.lit(numPercentiles-1))
      .otherwise(col)

    ds.withColumn(placeHolder, F.percent_rank().over(win))
      .withColumn(prefix(colName), capBy9(perCol))
      .drop(placeHolder)
  }

  def bucketLtc[T](ds: Dataset[T]): DataFrame = {
    val ltc = F.col("ltc")
    ds.withColumn(prefix("ltc"),
                  F.when(ltc <= F.lit(0), F.lit(0))
                   .when(ltc <= F.lit(100), F.lit(1))
                   .otherwise(F.lit(2)))
  }

  def generateRandom(df: DataFrame): DataFrame = {
    df.withColumn(prefix("randomNoise"), F.col("randomNoise"))
  }

  def encodeOriginCode(df: DataFrame): DataFrame = {
    val originCode = F.col("originCode")
    df.withColumn(
      prefix("originCode"),
      F.when(originCode === F.lit("IN"), 1)
       .when(originCode === F.lit("JP"), 2)
       .when(originCode === F.lit("TW"), 3)
       .when(originCode === F.lit("CN"), 4)
       .when(originCode === F.lit("TH"), 5)
       .when(originCode === F.lit("ID"), 6)
       .when(originCode === F.lit("KR"), 7)
       .when(originCode === F.lit("MY"), 8)
       .when(originCode === F.lit("PH"), 9)
       .when(originCode === F.lit("VN"), 10)
       .when(originCode === F.lit("SG") || originCode === F.lit("HK"), 11)
       .when(originCode === F.lit("US") || originCode === F.lit("CA"), 12)
       .when(originCode === F.lit("AU") || originCode == F.lit("NZ"), 13)
       .when(originCode === F.lit("DE") ||
             originCode === F.lit("GB") ||
             originCode === F.lit("FR") ||
             originCode === F.lit("IT") ||
             originCode === F.lit("PL") ||
             originCode === F.lit("NL") ||
             originCode === F.lit("ES") ||
             originCode === F.lit("IL") ||
             originCode === F.lit("AT") ||
             originCode === F.lit("SZ") ||
             originCode === F.lit("RO") ||
             originCode === F.lit("GR"), 14)
       .otherwise(0)
    )
  }

  def encodeDestCode(df: DataFrame): DataFrame = {
    val countryName = F.col("destinationCode")
    df.withColumn(
      prefix("destinationCode"),
      F.when(countryName === F.lit("Japan"), 1)
       .when(countryName === F.lit("Thailand"), 2)
       .when(countryName === F.lit("India"), 3)
       .when(countryName === F.lit("Indonesia"), 4)
       .when(countryName === F.lit("Taiwan"), 5)
       .when(countryName === F.lit("Malaysia"), 6)
       .when(countryName === F.lit("Vietnam"), 7)
       .when(countryName === F.lit("Philippines"), 8)
       .when(countryName === F.lit("South Korea"), 9)
       .when(countryName === F.lit("United Arab Emirates"), 10)
       .when(countryName === F.lit("Turkey"), 11)
       .when(countryName === F.lit("United States") ||
             countryName === F.lit("Canada") , 12)
       .when(countryName === F.lit("Singapore"), 13)
       .when(countryName === F.lit("Hong Kong SAR, China"), 14)
       .when(countryName === F.lit("Italy") ||
             countryName === F.lit("France") ||
             countryName === F.lit("United Kingdom") ||
             countryName === F.lit("Switzerland") ||
             countryName === F.lit("Austria") ||
             countryName === F.lit("Spain") ||
             countryName === F.lit("Germany") ||
             countryName === F.lit("Poland") ||
             countryName === F.lit("Greece") ||
             countryName === F.lit("Portugal") ||
             countryName === F.lit("Hungary"), 15)
       .when(countryName === F.lit("Australia") ||
             countryName === F.lit("New Zealand"), 16)
       .otherwise(0))
  }

  def encodeDefaultDate[T](ds: Dataset[T]): DataFrame = {
    ds.withColumn(prefix("defaultDate"),
                  F.when(F.col("defaultDate") === F.lit("selected"),
                         F.lit(false))
                   .otherwise(true))
  }

  def encodeDayOfWeek[T](ds: Dataset[T]): DataFrame = {
    val dayOfWeek = F.dayofweek(F.to_date(F.col("checkinDate"), "yyyyMMdd"))
    ds.withColumn(prefix("checkinDate"),
                  F.when(dayOfWeek === F.lit(1) ||
                         dayOfWeek === F.lit(6) ||
                         dayOfWeek === F.lit(7), 1)
                   .otherwise(dayOfWeek))
  }

  def encodeStarRating[T](ds: Dataset[T]): DataFrame = {
    val starRating = F.col("starRating")
    ds.withColumn(prefix("starRating"),
                  F.when(starRating < 0.0, 0)
                   .when(starRating < 3.0, 1)
                   .when(starRating < 4.0, 2)
                   .when(starRating < 5.0, 3)
                   .otherwise(4))
  }

  def encodeDeviceTy[T](ds: Dataset[T]): DataFrame = {
    val devcieTy = F.col("deviceType")
    ds.withColumn(prefix("deviceType"),
                  F.when(devcieTy === F.lit("desktop"), 0)
                   .when(devcieTy === F.lit("mobile"), 1)
                   .otherwise(2)) // Tablet.
  }

  def encodeLos[T](ds: Dataset[T]): DataFrame = {
    ds.withColumn(prefix("lengthOfStay"),
                  F.when(F.col("lengthOfStay") === F.lit(1), 0)
                   .otherwise(1))
  }

  private def toCanonicalForm(df: DataFrame, discreteCols: Column*)
    : DataFrame = {
    df.select(F.array(discreteCols:_*).as("features"),
              F.col(prefix("ltc")).as("target"))
  }

  def pricingBayesianComponent(
    spark: SparkSession,
    dsPricing: Dataset[MatchingFeatures]): Dataset[ComponentDiscrete] = {
      import spark.implicits._

      //
      // Columns encoding.
      //
      val dfDiscrits = dsPricing
        .toDF
        .transform(generateRandom)
        .transform(encodeOriginCode)
        .transform(encodeDestCode)
        .transform(encodeDeviceTy)
        .transform(encodeLos)
        .transform(encodeDefaultDate)
        .transform(encodeDayOfWeek)
        .transform(encodeStarRating)
        .transform(bucketLtc)

      //
      // Schema conversion.
      //
      val discreteCols = TypeUtils
        .getFieldOrder(MatchingFeatures())
        .sortBy(p => p._2)
        .map(p => p._1)
        .map(c => F.col(prefix(c)).cast("int"))

      dfDiscrits
        .transform(df => toCanonicalForm(df, discreteCols:_*))
        .as[ComponentDiscrete]
  }
}
