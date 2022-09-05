package com.hg.alg.config

object NebulaConfig {

  def getReadNebula(configs: Configs): NebulaReadConfigEntry = {
    val nebulaConfigs: NebulaConfigEntry = configs.nebulaConfig
    nebulaConfigs.readConfigEntry
  }

  def getWriteNebula(configs: Configs): NebulaWriteConfigEntry = {
    val nebulaConfigs: NebulaConfigEntry = configs.nebulaConfig
    nebulaConfigs.writeConfigEntry
  }
}
