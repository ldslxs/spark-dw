package lds.bigdata.ods_etl

import lds.bigdata.util.SparkUtils
import org.apache.spark.sql.SparkSession

object ToAdsFunnelRate {
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println("请传递一个日期参数,漏斗名称")
      System.exit(0)
    }
    val dt = args(0)
    val funnel_name = args(1)
    val spark: SparkSession = SparkUtils.getSparkSession("ToAdsFunnelRate")

    val sql =
      s"""
         |
         |insert overwrite table ads.funnel_rate
         |partition (dt='${dt}')
         |select funnel_name,step,user_count,
         |      round(user_count/lag(user_count,1,user_count) over(partition by funnel_name order by step)*100,2) conversion_rate,
         |      round(user_count/first_value(user_count) over(partition by funnel_name order by step)*100,2)      completion_rate,
         |      funnel_starttime,
         |      funnel_endtime
         |from dws.user_funnel_aggr where funnel_name = '${funnel_name}'
         |
         |""".stripMargin

    spark.sql(sql)

    spark.stop()
}}
