package lds.bigdata.userstats

import lds.bigdata.util.SparkUtils

object DWSNewMidDay {
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println("请传递一个日期参数")
      System.exit(0) //退出虚拟机，说白了就是不执行后续代码
    }
    val dt = args(0)

    val spark = SparkUtils.getSparkSession("DWSNewMidDay")
    spark.sql(
      s"""
         |
         |insert into table dws.new_mid_day
         |select
         |    ud.deviceid  deviceid,
         |    '${dt}'  create_date
         |from dws.app_user_active_day ud left join dws.new_mid_day nm on ud.deviceid=nm.deviceid
         |where ud.dt='${dt}' and nm.deviceid is null
         |""".stripMargin)
    spark.stop()
  }
}
