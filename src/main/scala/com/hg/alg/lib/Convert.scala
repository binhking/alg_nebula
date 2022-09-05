package com.hg.alg.lib

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, dense_rank}

object Convert {
  def convertStringId2LongId(dataframe: DataFrame, path: String): DataFrame = {
    // 从边中获取所有顶点 ID
    val srcIdDF: DataFrame = dataframe.select("_srcId").withColumnRenamed("_srcId", "id")
    val dstIdDF: DataFrame = dataframe.select("_dstId").withColumnRenamed("_dstId", "id")
    val idDF: Dataset[Row] = srcIdDF.union(dstIdDF).distinct()

    // 使用dense_rank将id编码为Long类型，encodeId有两列：id，encodedId
    // 然后你需要保存 encodeId 以转换回算法的结果。
    val encodeId: DataFrame = idDF.withColumn("encodedId", dense_rank().over(Window.orderBy("id")))
    encodeId.write.option("header", value = true).csv(path)

    // 转换边数据的 src 和 dst
    val srcJoinDF: DataFrame = dataframe
      .join(encodeId)
      .where(col("_srcId") === col("id"))
      .drop("_srcId")
      .drop("id")
      .withColumnRenamed("encodedId", "_srcId")
    srcJoinDF.cache()
    val dstJoinDF: DataFrame = srcJoinDF
      .join(encodeId)
      .where(col("_dstId") === col("id"))
      .drop("_dstId")
      .drop("id")
      .withColumnRenamed("encodedId", "_dstId")

    dstJoinDF
  }

  def reconvertLongId2StringId(spark: SparkSession, dataframe: DataFrame, path: String): DataFrame = {
    // String id 和 Long id 映射数据
    val encodeId: DataFrame = spark.read.option("header", value = true).csv(path)

    encodeId
      .join(dataframe)
      .where(col("encodedId") === col("_id"))
      .drop("encodedId")
      .drop("_id")
  }

}
