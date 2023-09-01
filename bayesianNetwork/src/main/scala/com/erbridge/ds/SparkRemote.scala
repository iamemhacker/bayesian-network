package com.erbridge.ds

import org.apache.spark.sql.{SparkSession}
import com.typesafe.config.ConfigFactory


trait SparkRemote extends Serializable {
  def appName(): String 

  lazy val config = ConfigFactory.load()

  lazy val spark: SparkSession = {
      throw new Exception("Not yet implemented, cloud specific")
  }
}
