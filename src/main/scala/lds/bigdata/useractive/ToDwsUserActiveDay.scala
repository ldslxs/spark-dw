package lds.bigdata.useractive

import lds.bigdata.util.SparkUtils

object ToDwsUserActiveDay {
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println("请传递一个日期参数")
      System.exit(0) //退出虚拟机，说白了就是不执行后续代码
    }
    val dt = args(0)

    val spark = SparkUtils.getSparkSession("ToDwsUserActiveDay")
    spark.sql(
      s"""
         |
         |insert overwrite table dws.app_user_active_day
         |partition(dt='${dt}')
         |select  deviceid
         |from dwd.event_log_detail
         |where dt = '${dt}'
         |group by deviceid
         |
         |""".stripMargin)
    spark.stop()
  }
}
