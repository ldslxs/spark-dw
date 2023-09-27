package lds.bigdata.ods_etl

import lds.bigdata.util.SparkUtils
import org.apache.spark.sql.SparkSession

object AppLogSessionSplit {
  def main(args: Array[String]): Unit = {

      if (args.length == 0) {
        println("请输入时间,格式为(yyyy-mm-dd)")
        System.exit(0)
      }
      val dt = args(0)

      val spark: SparkSession = SparkUtils.getSparkSession("session切割")

      spark.sql(
        s"""
           |
           |insert overwrite table  tmp.event_log_splited
           |partition (dt='${dt}')
           |select   account             ,
           |         appid               ,
           |         appversion          ,
           |         carrier             ,
           |         deviceid            ,
           |         devicetype          ,
           |         eventid             ,
           |         ip                  ,
           |         latitude            ,
           |         longitude           ,
           |         nettype             ,
           |         osname              ,
           |         osversion           ,
           |         properties          ,
           |         releasechannel      ,
           |         resolution          ,
           |         sessionid           ,
           |         `timestamp`         ,
           |         concat_ws('-',sessionid,sum(ts) over (partition by sessionid order by `timestamp`))
           |         as newsessionid
           |         from(
           |select   *   ,
           |        if(`timestamp` - lag(`timestamp`, 1, `timestamp`) over  (partition by sessionid order by `timestamp`) > 15000, 1, 0)
           |       as ts
           |from tmp.event_log_washed where dt='${dt}'
           |)t1
           |""".stripMargin).show()

      spark.stop()
  }
}
