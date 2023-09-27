package lds.bigdata.useractive

import lds.bigdata.util.{MyDateUtils, SparkUtils}

object ToDwsUserActiveRange {
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println("请传递一个日期参数")
      System.exit(0) //退出虚拟机，说白了就是不执行后续代码
    }
    val dt = args(0)
    //获取前一天的日期
    val pre_dt: String = MyDateUtils.getYesterday(dt)

    val spark = SparkUtils.getSparkSession("ToDwsUserActiveRange")
    spark.sql(
      s"""
         |
         |insert into dws.app_user_active_range partition (dt='${dt}')
         |select if(t1.deviceid is null,t2.deviceid,t1.deviceid) deviceid,
         |       if(first_login is null,t2.dt,first_login) first_login,
         |       if(start_dt is null,t2.dt,start_dt) start_dt,
         |--        if(end_dt='9999-12-31' and dt2='2022-11-04','9999-12-31',
         |--            if(end_dt='9999-12-31' and dt2 is null,'2022-11-03',
         |--                if(end_dt is null,'9999-12-31',end_dt)) )end_dt
         |--         case when end_dt='9999-12-31' and dt2='2022-11-04' then '9999-12-31'//else 亦包括
         |       case      when end_dt='9999-12-31' and t2.dt is null then '${pre_dt}'
         |             when end_dt is null then '9999-12-31' else end_dt end as end_dt
         |
         |    from(
         |select  deviceid,first_login first_login,start_dt start_dt,
         |        end_dt end_dt, dt from dws.app_user_active_range where dt='${pre_dt}'
         |                                                         and end_dt>=date_sub('${dt}',30)
         |          )t1 full join dws.app_user_active_day t2 on t1.deviceid=t2.deviceid
         |union all
         |select t1.deviceid,first_login  ,'${dt}' as start_dt,
         |      '9999-12-31' as  end_dt
         |    from (select deviceid, first_login
         |             from tmp.app_user_active_range
         |             where dt = '${pre_dt}'
         |             group by deviceid, first_login
         |             having max(end_dt) != '9999-12-31')t1  join
         |            (select deviceid from tmp.app_user_active_day where dt='${dt}')t2
         |            on t1.deviceid=t2.deviceid
         |
         |""".stripMargin)
    spark.stop()
  }
}
