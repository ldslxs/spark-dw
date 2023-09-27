package lds.bigdata.useractive

import lds.bigdata.util.SparkUtils
import org.apache.spark.sql.SparkSession

object ToAdsUserActiveDayRange {
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println("请传递一个日期参数")
      System.exit(0) //退出虚拟机，说白了就是不执行后续代码
    }

    val dt = args(0)

    val spark: SparkSession = SparkUtils.getSparkSession("ToAdsUserActiveDayRange")

    spark.sql(
      s"""
         |
         |insert  into  ads.user_continuous_section
         |partition (dt='${dt}')
         |select
         |
         |       count(distinct if(x1>=1 and x1<10,deviceid,null))   active_1day_10day,
         |       count(distinct if(x1>=10 and x1<20,deviceid,null))  active_10day_20day,
         |       count(distinct if(x1>=20,deviceid,null))            active_20day_plus
         |from(
         |    select deviceid,
         |            datediff( if(end_dt = '9999-12-31',dt,end_dt) ,
         |                             if(start_dt>=date_sub('${dt}',30),start_dt,
         |                                 date_sub('${dt}',30)) ) + 1 x1
         |    from dws.app_user_active_range
         |    where dt = '${dt}'  and
         |                end_dt >= date_sub('${dt}',30)
         |)t1
         |
         |""".stripMargin)

    spark.stop()
  }
}
