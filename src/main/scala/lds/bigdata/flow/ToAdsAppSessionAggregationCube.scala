package lds.bigdata.flow

import lds.bigdata.util.SparkUtils
import org.apache.spark.sql.SparkSession

object ToAdsAppSessionAggregationCube {
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println("请传递一个日期参数")
      System.exit(0)
    }
    val dt = args(0)

    val spark: SparkSession = SparkUtils.getSparkSession("ToAdsAppSessionAggregationCube")

    spark.sql(
      s"""
         |
         |insert into ads.app_session_aggregation_cube
         |partition (dt='${dt}')
         |select province,city,enter_url,devicetype,
         |       sum(session_time) as total_session_time,
         |       count(session_time) as total_session_cnt,
         |       count(distinct deviceid)as device_cnt,
         |    round(count(session_time)/count(distinct deviceid),2)   avg_session_cnt,
         |    round(sum(session_time)/count(distinct deviceid),2)   avg_session_time
         |       from dws.app_session_aggregation where dt='${dt}'
         |group by devicetype,enter_url,province,city
         |grouping sets (province,(province,city),(province,devicetype),enter_url,devicetype,(province,enter_url))
         |
         |""".stripMargin)
    spark.stop();
  }
}
