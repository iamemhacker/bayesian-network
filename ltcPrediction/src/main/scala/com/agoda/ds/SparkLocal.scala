package com.agoda.ds

import org.apache.spark.sql.{SparkSession}

trait SparkLocal extends Serializable {
  def appName(): String 

  lazy val spark: SparkSession = {
    SparkSession.builder().appName(appName()).getOrCreate()
  }
}
