package com.agoda.ds

import scala.util.{Try, Success, Failure}
import org.apache.spark.sql.types.{StructType, StringType, IntegerType,
  StructField}
import org.apache.spark.sql.{SparkSession, functions => F, DataFrame, Dataset}
import com.agoda.ds.types._



trait TablesAPI {
  def hotels(): DataFrame

  def bookings(): DataFrame

  def trivagoBensMatching(): DataFrame
  
  def trivagoTrainData(): DataFrame

  def trivagoTrainDataWAdjustingWin(): Try[DataFrame]

  def trivagoWinLen(): Try[DataFrame]

  def tripadvisorBensMatching(): DataFrame

  def variableCost(): DataFrame

  def hpaBensMatching(): DataFrame
}

class ApiFactory(spark: SparkSession) {

  val HOTELS_STATIC = "bi_dw.dim_hotel_static"
  val FACT_BOOKINGS = "bi_dw.fact_booking-sample"
  val TRIVAGO_BENS_MATCHING = "trivago.bens_matching"
  val TRIVAGO_ALLOCATION = "trivago.allocation"
  val TRIVAGO_TRAIN = "trivago.ltmpc_with_modifiers_train_v3"
  val TRIVAGO_TRAIN_W_WIN = "trivago.adjusting_window"
  val TRIVAGO_WIN_LEN = "trivago.win_len"
  val PARTNER_VARIABLE_COST = "bi_partner.fact_booking_variable_cost"

  val TRIPADVISOR_BENS_MATCHING = "tripadvisor.bens_matching"
  val HPA_BENS_MATCHING = "hpa.bens_matching"

  def createLocal(dirPath: String): TablesAPI = {
    return new TablesAPI() {
      def hotels(): DataFrame = {
        spark.read.parquet(s"${dirPath}/${HOTELS_STATIC}/")
      }

      def bookings(): DataFrame = {
        spark.read.parquet(s"${dirPath}/${FACT_BOOKINGS}/")
      }

      def trivagoBensMatching(): DataFrame = {
        spark.read.parquet(s"${dirPath}/${TRIVAGO_BENS_MATCHING}/")
      }

      def trivagoAllocation(): DataFrame = {
        spark.read.parquet(s"${dirPath}/${TRIVAGO_ALLOCATION}/")
      }

      def trivagoTrainData(): DataFrame = {
        spark.read.parquet(s"${dirPath}/${TRIVAGO_TRAIN}/")
      }

      def trivagoTrainDataWAdjustingWin(): Try[DataFrame] = {
        Try(spark.read.parquet(s"${dirPath}/${TRIVAGO_TRAIN_W_WIN}/"))
      }

      def trivagoWinLen(): Try[DataFrame] = {
        Try(spark.read.parquet(s"${dirPath}/${TRIVAGO_WIN_LEN}/"))
      }

      def tripadvisorBensMatching(): DataFrame = {
        spark.read.parquet(s"${dirPath}/${TRIPADVISOR_BENS_MATCHING}/")
      }

      def hpaBensMatching(): DataFrame = {
        spark.read.parquet(s"${dirPath}/${HPA_BENS_MATCHING}/")
      }

      def variableCost(): DataFrame = {
        spark.read.parquet(s"${dirPath}/${PARTNER_VARIABLE_COST}/")
      }
    }
  }

  def createRemote(): TablesAPI = {
    return new TablesAPI() {
      def hotels(): DataFrame = {
        spark.table(HOTELS_STATIC)
      }

      def bookings(): DataFrame = {
        spark.table(FACT_BOOKINGS)
      }
      
      def trivagoBensMatching(): DataFrame = {
        spark.table(TRIVAGO_BENS_MATCHING)
      }

      def trivagoAllocation(): DataFrame = {
        spark.table(TRIVAGO_ALLOCATION)
      }

      def trivagoTrainData(): DataFrame = {
        spark.table(TRIVAGO_TRAIN)
      }

      def trivagoTrainDataWAdjustingWin(): Try[DataFrame] = {
        // TODO: hard coded dir. should this stick, we will register it as a
        // table.
        val path = s"""
          |/user/AGODA+emalul/out/
          |${TRIVAGO_TRAIN_W_WIN}/"""
          .stripMargin('|').replaceAll("\n", "")

        Try(spark.read.parquet(path))
      }
      
      def trivagoWinLen(): Try[DataFrame] = {
        val schema = StructType(
          StructField("hotel_id", IntegerType, true) ::
          StructField("origin_code", StringType, true) ::
          StructField("clickdate", IntegerType, true) ::
          StructField("window_begin", IntegerType, true) :: Nil
        )

        val path = s"""
          |/user/AGODA+emalul/out/
          |${TRIVAGO_WIN_LEN}/"""
          .stripMargin('|').replaceAll("\n", "")

        Try(spark.read.option("header", true).schema(schema).csv(path))
      }

      def tripadvisorBensMatching(): DataFrame = {
        spark.table(TRIPADVISOR_BENS_MATCHING)
      }

      def variableCost(): DataFrame = {
        spark.table(PARTNER_VARIABLE_COST)
      }
      
      def hpaBensMatching(): DataFrame = {
        spark.table(HPA_BENS_MATCHING)
      }
    }
  }
}
