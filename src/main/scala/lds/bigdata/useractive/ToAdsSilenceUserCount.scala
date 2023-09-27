package lds.bigdata.useractive

import lds.bigdata.util.SparkUtils
import org.apache.spark.sql.SparkSession

object ToAdsSilenceUserCount {
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println("请传递一个日期参数")
      System.exit(0) //退出虚拟机，说白了就是不执行后续代码
    }

    val dt = args(0)

    val spark: SparkSession = SparkUtils.getSparkSession("ToAdsSilenceUserCount")

    spark.sql(
      s"""
         |insert into ads.user_active_slience partition (dt='${dt}')
         |select count(if(x1>3,deviceid,null)) as silence_3,
         |       count(if(x1>5,deviceid,null)) as silence_5 from(
         |select deviceid,sum(x1) x1 from (
         |select deviceid, if(x1<0,0,x1) as x1
         |from (select deviceid,
         |             first_login,
         |             start_dt,
         |             end_dt,
         |             datediff(start_dt, lag(end_dt, 1,
         |                                    if(first_login < date_sub(dt, 30), date_sub(dt, 30), first_login)
         |                 ) over (partition by deviceid order by start_dt)) -
         |             1 as x1 --沉默时间段
         |      from dws.app_user_active_range
         |      where dt = '${dt}'
         |        and end_dt >= date_sub('${dt}', 30)) t1
         |union all
         |
         |select
         |    deviceid,
         |    datediff('${dt}',max(end_dt)) x2 from dws.app_user_active_range
         |  where dt = '${dt}' and
         |                      end_dt >= date_sub('${dt}',30)
         |        group by deviceid having max(end_dt) != '9999-12-31'
         |union all
         |select deviceid,30 as x1 from(
         |select deviceid,dt, max(end_dt) m1 from dws.app_user_active_range group by deviceid,dt)t1
         |where m1<date_sub(dt,30)) t2  group by deviceid ) t3 where x1>0
         |""".stripMargin)

    spark.stop()
  }
}
