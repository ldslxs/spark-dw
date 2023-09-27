package lds.bigdata.ods_etl
import lds.bigdata.test.Funnel
import lds.bigdata.util.{ConfigUtils, SparkUtils}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import java.util.Properties
object ToDwsUserFunnelAggr {
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println("请传递一个日期参数,漏斗名称")
      System.exit(0)
    }
    val dt = args(0)
    val funnel_name = args(1)
    val spark: SparkSession = SparkUtils.getSparkSession("ToDwsUserFunnelAggr")
    import spark.implicits._
    //1.主函数接收两个参数 参数1：计算时间 参数2：漏斗名称
    //2.根据漏斗名称查询mysql数据库t_funnel表
    //3.使用动态SQL,拼接

    val properties = new Properties()
    properties.setProperty("driver", ConfigUtils.driverClass)
    properties.setProperty("user", ConfigUtils.username)
    properties.setProperty("password", ConfigUtils.password)

    val ds: Dataset[Row] = spark.read.jdbc(ConfigUtils.url, "t_funnel", properties).where($"funnel_name" === funnel_name)
    val list: Array[Funnel] = ds.rdd.map(row => {
      val id = row.getAs[Int]("id")
      val funnel_name = row.getAs[String]("funnel_name")
      val step = row.getAs[Int]("step")
      val eventid = row.getAs[String]("eventid")
      val eventname = row.getAs[String]("eventname")
      Funnel(id, funnel_name, step, eventid, eventname)
    }).collect()

    var str = ""
    var str2 = ""
    var i = 1
    for (elem <- list) {
      if (i == list.size) {
        str += s"sum(if(max_step>=${i},1,0)) x${i}"
        str2 +=
          s"""|select funnel_name,${i} step,x${i},funnel_starttime,funnel_endtime
              |from t1
              |""".stripMargin
      } else {
        str += s"sum(if(max_step>=${i},1,0)) x${i},"
        str2 +=
          s"""|select funnel_name,${i} step,x${i},funnel_starttime,funnel_endtime
              |from t1
              |union all
              |""".stripMargin
      }
      i += 1
    }

    val sql =
      s"""
         |
         |with t1 as (
         |    select funnel_name,funnel_starttime,funnel_endtime,
         |         ${str}
         |    from dws.user_buy_funnel
         |    where funnel_name = '${funnel_name}'
         |    group by funnel_name,funnel_starttime,funnel_endtime
         |)
         |insert overwrite table dws.user_funnel_aggr
         |partition (dt='${dt}')
         |${str2}
         |
         |""".stripMargin

    spark.sql(sql)

    spark.stop()
  }
}