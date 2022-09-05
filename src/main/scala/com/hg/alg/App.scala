package com.hg.alg


import com.hg.alg.config.{Configs, SparkConfig}
import com.hg.alg.config.Configs.Argument
import com.hg.alg.lib.Convert.{convertStringId2LongId, reconvertLongId2StringId}
import com.hg.alg.reader.{CsvReader, NebulaReader}
import com.hg.alg.writer.{CsvWriter, NebulaWriter}
import com.vesoft.nebula.algorithm.config.{CcConfig, LouvainConfig, PRConfig}
import com.vesoft.nebula.algorithm.lib.{LouvainAlgo, PageRankAlgo, StronglyConnectedComponentsAlgo}
import org.apache.commons.math3.ode.UnknownParameterException
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @author ${user.name}
 */
object App {

  private val LOGGER: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]) {
    val PROGRAM_NAME = "Nebula graphx"
    val options: Option[Argument] = Configs.parser(args, PROGRAM_NAME)
    val p: Argument = options match {
      case Some(config) => config
      case _ =>
        LOGGER.error("Argument parse failed")
        sys.exit(-1)
    }
    val configs: Configs = Configs.parse(p.config)
    LOGGER.info(s"configs =  ${configs}")
    println(p.encodePath)

    val sparkConfig: SparkConfig = SparkConfig.getSpark(configs)
    val partitionNum: String = sparkConfig.partitionNum
    sparkConfig.spark.sparkContext.setLogLevel("ERROR")

    // reader
    val dataSet: DataFrame = createDataSource(sparkConfig.spark, configs, partitionNum)

    println("======================dataSet======================")
    dataSet.show()
    // algorithm
    val algoName: String = {
      val algoConfig: Map[String, String] = configs.algorithmConfig.map
      algoConfig("algorithm.executeAlgo")
    }

    val convertDataFrame: DataFrame = convertStringId2LongId(dataSet, p.encodePath).select("_srcId", "_dstId")

    val algoResult: DataFrame = executeAlgorithm(sparkConfig.spark, algoName, configs, convertDataFrame)

    val decodedPr: DataFrame = reconvertLongId2StringId(sparkConfig.spark, algoResult, p.encodePath)
    println("======================decodedPr======================")
    decodedPr.show(1000)
    // writer
    saveAlgoResult(decodedPr, configs)
    sparkConfig.spark.stop()
    sys.exit(0)
  }


  /**
   * create data from datasource
   *
   * @param spark   SparkSession
   * @param configs Configs
   * @return DataFrame
   */
  private[this] def createDataSource(spark: SparkSession,
                                     configs: Configs,
                                     partitionNum: String): DataFrame = {


    val dataSource: String = configs.dataSourceSinkEntry.source
    val dataSet: Dataset[Row] = dataSource.toLowerCase match {
      case "nebula" =>
        val reader = new NebulaReader(spark, configs, partitionNum)
        reader.read()
      case "csv" =>
        val reader = new CsvReader(spark, configs, partitionNum)
        reader.read()
    }
    dataSet
  }

  private[this] def executeAlgorithm(spark: SparkSession,
                                     algoName: String,
                                     configs: Configs,
                                     dataSet: DataFrame): DataFrame = {
    val hasWeight: Boolean = configs.dataSourceSinkEntry.hasWeight
    val algoConfig: Map[String, String] = configs.algorithmConfig.map
    val algoResult: DataFrame = {
      algoName.toLowerCase match {
        case "louvain" =>
          val maxIter: Int = algoConfig("algorithm.louvain.maxIter").toInt
          val internalIter: Int = algoConfig("algorithm.louvain.internalIter").toInt
          val tol: Double = algoConfig("algorithm.louvain.tol").toDouble
          val lovingConfig: LouvainConfig = LouvainConfig(maxIter, internalIter, tol)
          LouvainAlgo.apply(spark, dataSet, lovingConfig, hasWeight)
        case "scc" =>
          val maxIter: Int = algoConfig("algorithm.scc.maxIter").toInt
          val ccConfig: CcConfig = CcConfig(maxIter)
          StronglyConnectedComponentsAlgo.apply(spark, dataSet, ccConfig, hasWeight)
        case "pagerank" =>
          val maxIter: Int = algoConfig("algorithm.pagerank.maxIter").toInt
          val resetProb: Double = algoConfig("algorithm.pagerank.resetProb").toDouble
          val pageRankConfig: PRConfig = PRConfig(maxIter, resetProb)
          PageRankAlgo.apply(spark, dataSet, pageRankConfig, hasWeight)
        case _ => throw new UnknownParameterException("unknown executeAlgo name.")
      }
    }
    algoResult
  }

  private[this] def saveAlgoResult(algoResult: DataFrame, configs: Configs): Unit = {
    val dataSink: String = configs.dataSourceSinkEntry.sink
    dataSink.toLowerCase match {
      case "nebula" =>
        val writer = new NebulaWriter(algoResult, configs)
        writer.write()
      case "csv" =>
        val writer = new CsvWriter(algoResult, configs)
        writer.write()

      case _ => throw new UnsupportedOperationException("unsupported data sink")
    }
  }
}




