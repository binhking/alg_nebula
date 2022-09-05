package com.hg.alg.reader

import com.hg.alg.config.Configs
import com.vesoft.nebula.connector.connector.NebulaDataFrameReader
import com.vesoft.nebula.connector.{NebulaConnectionConfig, ReadNebulaConfig}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

abstract class DataReader(spark: SparkSession, configs: Configs) {
  def read(): DataFrame
}

class NebulaReader(spark: SparkSession, configs: Configs, partitionNum: String)
  extends DataReader(spark, configs) {
  override def read(): DataFrame = {
    val metaAddress: String = configs.nebulaConfig.readConfigEntry.address
    val space: String = configs.nebulaConfig.readConfigEntry.space
    val labels: List[String] = configs.nebulaConfig.readConfigEntry.labels

    val weights: List[String] = configs.nebulaConfig.readConfigEntry.weightCols
    val partition: Int = partitionNum.toInt

    val config: NebulaConnectionConfig =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress(metaAddress)
        .withConenctionRetry(2)
        .build()

    val noColumn: Boolean = weights.isEmpty

    var dataset: DataFrame = null
    for (i <- labels.indices) {
      val returnCols: ListBuffer[String] = new ListBuffer[String]
      if (configs.dataSourceSinkEntry.hasWeight && weights.nonEmpty) {
        returnCols.append(weights(i))
      }
      val nebulaReadEdgeConfig: ReadNebulaConfig = ReadNebulaConfig
        .builder()
        .withSpace(space)
        .withLabel(labels(i))
        .withNoColumn(noColumn)
        .withReturnCols(returnCols.toList)
        .withPartitionNum(partition)
        .build()
      if (dataset == null) {
        dataset = spark.read.nebula(config, nebulaReadEdgeConfig).loadEdgesToDF()
        if (weights.nonEmpty) {
          dataset = dataset.select("_srcId", "_dstId", weights(i))
        }
      } else {
        var df: DataFrame = spark.read.nebula(config, nebulaReadEdgeConfig).loadEdgesToDF()
        if (weights.nonEmpty) {
          df = df.select("_srcId", "_dstId", weights(i))
        }
        dataset = dataset.union(df)
      }
    }
    dataset
  }
}

class CsvReader(spark: SparkSession, configs: Configs, partitionNum: String)
  extends DataReader(spark, configs) {
  override def read(): DataFrame = {
    val delimiter: String = configs.localConfigEntry.delimiter
    val header: Boolean = configs.localConfigEntry.header
    val localPath: String = configs.localConfigEntry.filePath

    val partition: Int = partitionNum.toInt

    val data: DataFrame =
      spark.read
        .option("header", header)
        .option("delimiter", delimiter)
        .csv(localPath)
    val weight: String = configs.localConfigEntry.weight
    val src: String = configs.localConfigEntry.srcId
    val dst: String = configs.localConfigEntry.dstId
    if (configs.dataSourceSinkEntry.hasWeight && weight != null && weight.trim.nonEmpty) {
      data.select(src, dst, weight)
    } else {
      data.select(src, dst)
    }
    if (partition != 0) {
      data.repartition(partition)
    }
    data
  }
}


