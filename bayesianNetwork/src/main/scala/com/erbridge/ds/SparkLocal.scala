package com.erbridge.ds

import org.apache.spark.sql.{SparkSession}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Logger}

trait SparkLocal extends Serializable {
  def appName(): String 

  lazy val config = ConfigFactory.load()

  lazy val spark: SparkSession = {
    SparkSession.builder().appName(appName()).getOrCreate()
  }

  lazy val logger: Logger = Logger.getRootLogger()
}
