package lds.bigdata.flow

import lds.bigdata.util.SparkUtils
import org.apache.spark.sql.SparkSession

object ToAdsVisitCntAnalysis {
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println("请传递一个日期参数")
      System.exit(0)
    }

    val dt = args(0)

    val spark: SparkSession = SparkUtils.getSparkSession("ToAdsVisitCntAnalysis")

    spark.sql(
      s"""
         |
         |insert into ads.visit_cnt_analysis partition (dt='${dt}')
         |select province,count(*) as session_cnt,count(distinct deviceid)  device_cnt,
         |           round(count(*)/count(distinct deviceid),2)   avg_session_cnt
         | from dws.app_session_aggregation
         |where dt = '${dt}'
         |group by province
         |""".stripMargin)

    spark.stop()
  }
}
