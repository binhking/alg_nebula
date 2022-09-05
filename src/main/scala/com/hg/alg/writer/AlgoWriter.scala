package com.hg.alg.writer

import com.hg.alg.config.Configs
import com.vesoft.nebula.algorithm.config.AlgoConstants
import com.vesoft.nebula.connector.connector.NebulaDataFrameWriter
import com.vesoft.nebula.connector.{NebulaConnectionConfig, WriteMode, WriteNebulaVertexConfig}
import org.apache.spark.sql.DataFrame

abstract class AlgoWriter(data: DataFrame, configs: Configs) {
  def write(): Unit
}

class NebulaWriter(data: DataFrame, configs: Configs) extends AlgoWriter(data, configs) {
  override def write(): Unit = {
    val graphAddress: String = configs.nebulaConfig.writeConfigEntry.graphAddress
    val metaAddress: String = configs.nebulaConfig.writeConfigEntry.metaAddress
    val space: String = configs.nebulaConfig.writeConfigEntry.space
    val tag: String = configs.nebulaConfig.writeConfigEntry.tag
    val user: String = configs.nebulaConfig.writeConfigEntry.user
    val passwd: String = configs.nebulaConfig.writeConfigEntry.password
    val writeType: String = configs.nebulaConfig.writeConfigEntry.writeType
    val writeMode: WriteMode.Value = if (writeType.equals("insert")) WriteMode.INSERT else WriteMode.UPDATE

    val config: NebulaConnectionConfig =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress(metaAddress)
        .withGraphAddress(graphAddress)
        .withConenctionRetry(2)
        .build()
    val nebulaWriteVertexConfig: WriteNebulaVertexConfig = WriteNebulaVertexConfig
      .builder()
      .withUser(user)
      .withPasswd(passwd)
      .withSpace(space)
      .withTag(tag)
      .withVidField(AlgoConstants.ALGO_ID_COL)
      .withVidAsProp(false)
      .withBatch(1000)
      .withWriteMode(writeMode)
      .build()
    data.write.nebula(config, nebulaWriteVertexConfig).writeVertices()
  }
}

class CsvWriter(data: DataFrame, configs: Configs) extends AlgoWriter(data, configs) {
  override def write(): Unit = {
    val resultPath: String = configs.localConfigEntry.resultPath
    println(resultPath)
    data.write.option("header", value = true).csv(resultPath)
  }
}


