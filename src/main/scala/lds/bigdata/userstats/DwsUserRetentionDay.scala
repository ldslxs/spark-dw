package lds.bigdata.userstats

import lds.bigdata.util.SparkUtils

object DwsUserRetentionDay {
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println("请传递一个日期参数")
      System.exit(0) //退出虚拟机，说白了就是不执行后续代码
    }
    val dt = args(0)

    val spark = SparkUtils.getSparkSession("DwsUserRetentionDay")
    spark.sql(
      s"""
         |
         |insert into dws.user_retention_day partition (dt='${dt}')
         |select t1.deviceid,
         |       create_date,1 retention_day
         |       from (select deviceid,create_date from dws.new_mid_day where create_date=date_sub('${dt}',1))t1
         |join (select deviceid,dt from dws.app_user_active_day where dt='${dt}')t2
         |on t1.deviceid=t2.deviceid
         |union all select t1.deviceid,
         |       create_date,2 retention_day
         |       from (select deviceid,create_date from dws.new_mid_day where create_date=date_sub('${dt}',2))t1
         |join (select deviceid,dt from dws.app_user_active_day where dt='${dt}')t2
         |on t1.deviceid=t2.deviceid
         |union all select t1.deviceid,
         |       create_date,3 retention_day
         |       from (select deviceid,create_date from dws.new_mid_day where create_date=date_sub('${dt}',3))t1
         |join (select deviceid,dt from dws.app_user_active_day where dt='${dt}')t2
         |on t1.deviceid=t2.deviceid
         |""".stripMargin)
    spark.stop()
  }
}
