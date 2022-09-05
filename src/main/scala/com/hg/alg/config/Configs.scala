package com.hg.alg.config

import java.io.File
import java.nio.file.Files

import scala.collection.JavaConverters._
import com.typesafe.config.{Config, ConfigFactory, ConfigObject}
import scopt.OptionParser

import scala.collection.mutable

/**
 * sparkConfig用于提交spark应用，比如图算法
 */
object SparkConfigEntry {
  def apply(config: Config): SparkConfigEntry = {
    val map: mutable.Map[String, String] = mutable.Map[String, String]()
    val sparkConfig: ConfigObject = config.getObject("spark")
    for (key <- sparkConfig.unwrapped().keySet().asScala) {
      val sparkKey = s"spark.${key}"
      if (config.getAnyRef(sparkKey).isInstanceOf[String]) {
        val sparkValue: String = config.getString(sparkKey)
        map += sparkKey -> sparkValue
      } else {
        for (scKey <- config.getObject(sparkKey).unwrapped().keySet().asScala) {
          val key = s"${sparkKey}.${scKey}"
          val sparkValue: String = config.getString(key)
          map += key -> sparkValue
        }
      }
    }
    SparkConfigEntry(map.toMap)
  }
}

/**
 * SparkConfigEntry 支持 Spark 会话的键值对。
 *
 * @param map 配置值
 */
case class SparkConfigEntry(map: Map[String, String]) {
  override def toString: String = {
    map.toString()
  }
}


/**
 * AlgorithmConfig 用于运行图算法
 */
object AlgorithmConfigEntry {
  def apply(config: Config): AlgorithmConfigEntry = {
    val map: mutable.Map[String, String] = mutable.Map[String, String]()
    val algoConfig: ConfigObject = config.getObject("algorithm")
    for (key <- algoConfig.unwrapped().keySet().asScala) {
      val algorithmKey = s"algorithm.${key}"
      if (config.getAnyRef(algorithmKey).isInstanceOf[String]) {
        val algorithmValue: String = config.getString(algorithmKey)
        map += algorithmKey -> algorithmValue
      } else {
        for (algKey <- config.getObject(algorithmKey).unwrapped().keySet().asScala) {
          val key = s"${algorithmKey}.${algKey}"
          val value: String = config.getString(key)
          map += key -> value
        }

      }
    }
    AlgorithmConfigEntry(map.toMap)
  }
}

/**
 * AlgorithmConfigEntry 支持算法的键值对。
 *
 * @param map 算法值
 */
case class AlgorithmConfigEntry(map: Map[String, String]) {
  override def toString: String = {
    map.toString()
  }
}


/**
 * NebulaConfig is used to read edge data
 */
object NebulaConfigEntry {
  def apply(config: Config): NebulaConfigEntry = {
    if (!config.hasPath("nebula")) {
      return NebulaConfigEntry(NebulaReadConfigEntry(), NebulaWriteConfigEntry())
    }
    val nebulaConfig: Config = config.getConfig("nebula")

    val readMetaAddress: String = nebulaConfig.getString("read.metaAddress")
    val readSpace: String = nebulaConfig.getString("read.space")
    val readLabels: List[String] = nebulaConfig.getStringList("read.labels").asScala.toList
    val readWeightCols: List[String] = if (nebulaConfig.hasPath("read.weightCols")) {
      nebulaConfig.getStringList("read.weightCols").asScala.toList
    } else {
      List()
    }
    val readConfigEntry: NebulaReadConfigEntry =
      NebulaReadConfigEntry(readMetaAddress, readSpace, readLabels, readWeightCols)

    val graphAddress: String = nebulaConfig.getString("write.graphAddress")
    val writeMetaAddress: String = nebulaConfig.getString("write.metaAddress")
    val user: String = nebulaConfig.getString("write.user")
    val password: String = nebulaConfig.getString("write.password")
    val writeSpace: String = nebulaConfig.getString("write.space")
    val writeTag: String = nebulaConfig.getString("write.tag")
    val writeType: String = nebulaConfig.getString("write.type")
    val writeConfigEntry: NebulaWriteConfigEntry =
      NebulaWriteConfigEntry(graphAddress,
        writeMetaAddress,
        user,
        password,
        writeSpace,
        writeTag,
        writeType)
    NebulaConfigEntry(readConfigEntry, writeConfigEntry)
  }
}

/**
 * NebulaConfigEntry
 *
 * @param readConfigEntry  config for nebula-spark-connector reader
 * @param writeConfigEntry config for nebula-spark-connector writer
 */
case class NebulaConfigEntry(readConfigEntry: NebulaReadConfigEntry,
                             writeConfigEntry: NebulaWriteConfigEntry) {
  override def toString: String = {
    s"NebulaConfigEntry:{${readConfigEntry.toString}, ${writeConfigEntry.toString}"
  }
}

case class NebulaReadConfigEntry(address: String = "",
                                 space: String = "",
                                 labels: List[String] = List(),
                                 weightCols: List[String] = List()) {
  override def toString: String = {
    s"NebulaReadConfigEntry: " +
      s"{address: $address, space: $space, labels: ${labels.mkString(",")},  " +
      s"weightCols: ${weightCols.mkString(",")}}"
  }
}

case class NebulaWriteConfigEntry(graphAddress: String = "",
                                  metaAddress: String = "",
                                  user: String = "",
                                  password: String = "",
                                  space: String = "",
                                  tag: String = "",
                                  writeType: String = "insert") {
  override def toString: String = {
    s"NebulaWriteConfigEntry: " +
      s"{graphAddress: $graphAddress, user: $user, password: $password, space: $space, tag: $tag, type: $writeType}"
  }
}

object LocalConfigEntry {
  def apply(config: Config): LocalConfigEntry = {

    var filePath: String = ""
    var src: String = ""
    var dst: String = ""
    var weight: String = null
    var resultPath: String = null
    var header: Boolean = false
    var delimiter: String = ","

    if (config.hasPath("local.read.filePath")) {
      filePath = config.getString("local.read.filePath")
      src = config.getString("local.read.srcId")
      dst = config.getString("local.read.dstId")
      if (config.hasPath("local.read.weight")) {
        weight = config.getString("local.read.weight")
      }
      if (config.hasPath("local.read.delimiter")) {
        delimiter = config.getString("local.read.delimiter")
      }
      if (config.hasPath("local.read.header")) {
        header = config.getBoolean("local.read.header")
      }
    }
    if (config.hasPath("local.write.resultPath")) {
      resultPath = config.getString("local.write.resultPath")
    }
    LocalConfigEntry(filePath, src, dst, weight, resultPath, header, delimiter)
  }
}

case class LocalConfigEntry(filePath: String,
                            srcId: String,
                            dstId: String,
                            weight: String,
                            resultPath: String,
                            header: Boolean,
                            delimiter: String) {
  override def toString: String = {
    s"LocalConfigEntry: {filePath: $filePath, srcId: $srcId, dstId: $dstId, " +
      s"weight:$weight, resultPath:$resultPath, delimiter:$delimiter}"
  }
}

/** DataSourceEntry is used to determine the data source , nebula or local */
object DataSourceSinkEntry {
  def apply(config: Config): DataSourceSinkEntry = {
    val dataSource: String = config.getString("data.source")
    val dataSink: String = config.getString("data.sink")
    val hasWeight: Boolean = if (config.hasPath("data.hasWeight")) {
      config.getBoolean("data.hasWeight")
    } else false
    DataSourceSinkEntry(dataSource, dataSink, hasWeight)
  }
}

/**
 * DataSourceEntry
 */
case class DataSourceSinkEntry(source: String, sink: String, hasWeight: Boolean) {
  override def toString: String = {
    s"DataSourceEntry: {source:$source, sink:$sink, hasWeight:$hasWeight}"
  }
}


case class Configs(sparkConfig: SparkConfigEntry,
                   dataSourceSinkEntry: DataSourceSinkEntry,
                   nebulaConfig: NebulaConfigEntry,
                   algorithmConfig: AlgorithmConfigEntry,
                   localConfigEntry: LocalConfigEntry)

object Configs {


  /**
   *
   * @param configPath 配置文件路径
   * @return
   */
  def parse(configPath: File): Configs = {
    if (!Files.exists(configPath.toPath)) {
      throw new IllegalArgumentException(s"${configPath} not exist")
    }

    val config: Config = ConfigFactory.parseFile(configPath)
    val localConfigEntry: LocalConfigEntry = LocalConfigEntry(config)
    val nebulaConfigEntry: NebulaConfigEntry = NebulaConfigEntry(config)
    val sparkEntry: SparkConfigEntry = SparkConfigEntry(config)
    val dataSourceEntry: DataSourceSinkEntry = DataSourceSinkEntry(config)
    val algorithmEntry: AlgorithmConfigEntry = AlgorithmConfigEntry(config)

    Configs(sparkEntry, dataSourceEntry, nebulaConfigEntry, algorithmEntry, localConfigEntry)
  }

  /**
   * 通过路径获取配置列表
   *
   * @param config The config.
   * @param path   The path of the config.
   * @return
   */
  private[this] def getConfigsOrNone(config: Config,
                                     path: String): Option[java.util.List[_ <: Config]] = {
    if (config.hasPath(path)) {
      Some(config.getConfigList(path))
    } else {
      None
    }
  }

  /**
   * 通过路径获取配置
   *
   * @param config The config.
   * @param path   The path of the config.
   * @return
   */
  def getConfigOrNone(config: Config, path: String): Option[Config] = {
    if (config.hasPath(path)) {
      Some(config.getConfig(path))
    } else {
      None
    }
  }

  /**
   * 通过路径从配置中获取值。 如果路径不存在，则返回默认值。
   *
   * @param config       The config.
   * @param path         The path of the config.
   * @param defaultValue The default value for the path.
   * @return
   */
  private[this] def getOrElse[T](config: Config, path: String, defaultValue: T): T = {
    if (config.hasPath(path)) {
      config.getAnyRef(path).asInstanceOf[T]
    } else {
      defaultValue
    }
  }

  private[this] def getOptOrElse(config: Config, path: String): Option[String] = {
    if (config.hasPath(path)) {
      Some(config.getString(path))
    } else {
      None
    }
  }

  /**
   * 通过可选的路径从配置中获取值
   * 如果路径不存在，则返回默认值
   *
   * @param config       The config.
   * @param path         The path of the config.
   * @param defaultValue The default value for the path.
   * @tparam T
   * @return
   */
  private[this] def getOrElse[T](config: Option[Config], path: String, defaultValue: T): T = {
    if (config.isDefined && config.get.hasPath(path)) {
      config.get.getAnyRef(path).asInstanceOf[T]
    } else {
      defaultValue
    }
  }

  final case class Argument(config: File = new File("application.conf"), encodePath: String = "")

  /**
   * 用于解析命令行参数
   *
   * @param args
   * @param programName
   * @return Argument
   */
  def parser(args: Array[String], programName: String): Option[Argument] = {
    val parser: OptionParser[Argument] = new scopt.OptionParser[Argument](programName) {
      head(programName, "1.0.0")

      opt[File]('p', "prop")
        .required()
        .valueName("<file>")
        .action((x: File, c: Argument) => c.copy(config = x))
        .text("config file")
      opt[String]('e', "encode")
        .required()
        .valueName("<file>")
        .action((x: String, c: Argument) => c.copy( encodePath= x))
        .text("encodeId file")

    }
    parser.parse(args, Argument())
  }
}

