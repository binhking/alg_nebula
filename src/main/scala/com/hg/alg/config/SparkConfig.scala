package com.hg.alg.config

import org.apache.spark.sql.SparkSession

case class SparkConfig(spark: SparkSession, partitionNum: String)

object SparkConfig {

  var spark: SparkSession = _

  var partitionNum: String = _

  def getSpark(configs: Configs, defaultAppName: String = "algorithm"): SparkConfig = {
    val sparkConfigs: Map[String, String] = configs.sparkConfig.map
    val session: SparkSession.Builder = SparkSession.builder
      .appName(defaultAppName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    for (key <- sparkConfigs.keySet) {
      session.config(key, sparkConfigs(key))
    }
    partitionNum = sparkConfigs.getOrElse("spark.app.partitionNum", "0")
    SparkConfig(session.getOrCreate(), partitionNum)
  }
}