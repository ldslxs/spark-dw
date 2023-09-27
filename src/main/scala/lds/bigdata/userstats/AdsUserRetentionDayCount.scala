package lds.bigdata.userstats

import lds.bigdata.util.SparkUtils

object AdsUserRetentionDayCount {
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println("请传递一个日期参数")
      System.exit(0) //退出虚拟机，说白了就是不执行后续代码
    }
    val dt = args(0)

    val spark = SparkUtils.getSparkSession("AdsUserRetentionDayCount")
    spark.sql(
      s"""
         |
         |insert into table ads.user_retention_day_count
         |select
         |    create_date,
         |    retention_day,
         |    count(*) retention_count
         |from dws.user_retention_day
         |where dt='${dt}'
         |group by create_date,retention_day
         |""".stripMargin)
    spark.stop()
  }
}
