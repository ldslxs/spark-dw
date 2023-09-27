package lds.bigdata.ods_etl

import lds.bigdata.util.SparkUtils
import org.apache.spark.sql.SparkSession

object AppLogWash {
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println("请传递一个日期参数")
      System.exit(0) //退出虚拟机，说白了就是不执行后续代码
    }
    var dt=args(0)
2
    val spark: SparkSession = SparkUtils.getSparkSession("dw1")


    spark.sql(
      s"""
         |insert  overwrite table tmp.event_log_washed
         |partition(dt='${dt}')
         | select
         |	account
         |	,appid
         |	,appversion
         |	,carrier
         |	,deviceid
         |	,devicetype
         |	,eventid
         |	,ip
         |	,latitude
         |	,longitude
         |	,nettype
         |	,osname
         |	,osversion
         |	,properties
         |	,releasechannel
         |	,resolution
         |	,sessionid
         |	,`timestamp`
         |from ods.app_event_log
         |where dt = '${dt}' and
         |      deviceid  is not null  and  trim(deviceid) != ''   and
         |      eventid  	is not null  and  trim(eventid) != ''   and
         |      sessionid is not null  and  trim(sessionid) != ''  and
         |	  properties is not null and size(properties) != 0
         |    and from_unixtime(cast(`timestamp`/1000 as bigint),'yyyy-MM-dd')='${dt}'
         |
         |""".stripMargin)
    spark.stop()

  }
}
