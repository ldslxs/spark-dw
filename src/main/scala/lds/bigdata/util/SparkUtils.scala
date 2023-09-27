package lds.bigdata.util

import org.apache.spark.sql.SparkSession

object SparkUtils {
  def getSparkSession(appName:String)={

    if (ConfigUtils.flag) {
      val spark: SparkSession = SparkSession.builder().appName(appName)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", 2).enableHiveSupport().getOrCreate()
      spark
    } else {
      val spark: SparkSession = SparkSession.builder().appName(appName)
        .master("yarn")
        .config("spark.sql.shuffle.partitions", 20).enableHiveSupport().getOrCreate()
      spark
    }}
}
