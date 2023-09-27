package lds.bigdata.userstats

import lds.bigdata.util.SparkUtils

object AdsUserRetentionDayRate {
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println("请传递一个日期参数")
      System.exit(0) //退出虚拟机，说白了就是不执行后续代码
    }
    val dt = args(0)

    val spark = SparkUtils.getSparkSession("AdsUserRetentionDayRate")
    spark.sql(
      s"""
         |
         |insert into ads.user_retention_day_rate
         |select  '${dt}',
         |        t1.create_date,
         |    t1.retention_day,
         |    t1.retention_count,
         |    t2.new_mid_count,
         |    t1.retention_count / t2.new_mid_count * 100
         |    from ads.user_retention_day_count t1 join ads.new_mid_count t2
         |on t1.create_date=t2.create_date and date_add(t1.create_date,t1.retention_day) = '${dt}'
         |
         |""".stripMargin)
    spark.stop()
  }
}
