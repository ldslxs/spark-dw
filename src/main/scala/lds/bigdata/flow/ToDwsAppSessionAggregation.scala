package lds.bigdata.flow

import lds.bigdata.util.SparkUtils
import org.apache.spark.sql.SparkSession

object ToDwsAppSessionAggregation {
  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      println("请传递一个日期参数")
      System.exit(0)
    }

    val dt = args(0)

    val spark: SparkSession = SparkUtils.getSparkSession("ToDwsAppSessionAggregation")

    spark.sql(
      s"""
         |
         |insert overwrite table dws.app_session_aggregation
         |partition(dt='${dt}')
         |select deviceid,newsessionid,province,city,district,devicetype,
         |       split(sort_array(collect_list(if(eventid = 'pageView',concat(`timestamp`,'_',properties['url']),null)))[0],'_')[1] enter_url,
         |       max(`timestamp`) - min(`timestamp`)   session_time
         |from dwd.event_log_detail where dt = '${dt}'
         |group by deviceid,newsessionid,province,city,district,devicetype
         |having  array_contains(collect_list(eventid),'pageView')
         |
         |""".stripMargin)

    spark.stop()
  }
}
