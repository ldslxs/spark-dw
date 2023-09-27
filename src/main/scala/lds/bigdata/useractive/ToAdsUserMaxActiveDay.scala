package lds.bigdata.useractive

import lds.bigdata.util.SparkUtils
import org.apache.spark.sql.SparkSession

object ToAdsUserMaxActiveDay {
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println("请传递一个日期参数")
      System.exit(0) //退出虚拟机，说白了就是不执行后续代码
    }

    val dt = args(0)

    val spark: SparkSession = SparkUtils.getSparkSession("ToAdsUserMaxActiveDay")

    spark.sql(
      s"""
         |
         |insert into ads.user_max_continuous_days
         |partition (dt='${dt}')
         |select deviceid,max(x1) active_max_day_cnt
         |from(
         |    select deviceid,
         |            datediff( if(end_dt = '9999-12-31',dt,end_dt) ,
         |                             if(start_dt>=date_sub('${dt}',30),start_dt,
         |                                 date_sub('${dt}',30)) ) + 1 x1
         |    from dws.app_user_active_range
         |    where dt = '${dt}'  and
         |                end_dt >= date_sub('${dt}',30)
         |)t1 group by deviceid
         |
         |""".stripMargin)

    spark.stop()

  }

}
