package lds.bigdata.ods_etl

import lds.bigdata.test.Funnel
import lds.bigdata.util.{ConfigUtils, SparkUtils}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import java.util.Properties

/**
 * create table dws.user_buy_funnel(
      deviceid          string,    -- 设备编号(用户id)
      funnel_name       string,    -- 漏斗名称 [自定义]
      max_step          int,       -- 最大完成该漏斗的步骤
      funnel_starttime  string,    -- 漏斗统计数据的窗口开始时间
      funnel_endtime    string     -- 漏斗统计数据的窗口结束时间
  )partitioned by (dt string);

  mysql中表
  create table t_funnel(
    id int primary key auto_increment,
    funnel_name varchar (50),
    step int,
    eventid varchar (50),
    eventname varchar (50)
  )

  id funnel_name step  eventid     eventname
  1   测试1        1    display     浏览商品
  2   测试1        2    addCart     添加购物车
  3   测试1        3    order       下订单
  4   测试1        4    pay         支付
  5   测试2        1    click_adv   点击广告
  6   测试2        2    activity    参与活动
  7   测试2        3    pay         支付
 */
object ToDwsUserBuyFunnel {
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println("请传递一个日期参数,漏斗名称")
      System.exit(0)
    }
    val dt = args(0)
    val start = args(1)
    val end = args(2)
    val funnel_name = args(3)
    val spark: SparkSession = SparkUtils.getSparkSession("ToDwsUserBuyFunnel")
    import spark.implicits._
    //1.主函数接收两个参数 参数1：计算时间  参数2：开始时间 参数3：结束时间  参数4：漏斗名称
    //2.根据漏斗名称查询mysql数据库t_funnel表
    //3.使用动态SQL,拼接

    val properties = new Properties()
    properties.setProperty("driver",ConfigUtils.driverClass)
    properties.setProperty("user",ConfigUtils.username)
    properties.setProperty("password",ConfigUtils.password)

    val ds: Dataset[Row] = spark.read.jdbc(ConfigUtils.url, "t_funnel", properties)
      .where($"funnel_name" === funnel_name)
    val list: Array[Funnel] = ds.rdd.map(row => {
      val id = row.getAs[Int]("id")
      val funnel_name = row.getAs[String]("funnel_name")
      val step = row.getAs[Int]("step")
      val eventid = row.getAs[String]("eventid")
      val eventname = row.getAs[String]("eventname")
      Funnel(id, funnel_name, step, eventid, eventname)
    }).collect()

    var str = ""        //拼接case when
    var str2 = ""      //拼接in后边的条件值
    var j = list.size
    for (s1 <- list) {        //1.外层循环拼接when  ... then
      var str1 = ""   //拼接具体的事件
      for (elem <- 0 until j) {    //2.内层循环拼接 when 后边的条件，因为每一个when后边的条件不同
        str1 += s".*(${list(elem).eventid}):"
      }
      str1 = str1.substring(0, str1.length - 1)         //去掉when后边条件多拼接的:
      str += s"when  regexp_extract(c1,'${str1}',${j}) = '${list(j - 1).eventid}' then ${j} \n"
      str2 += s"'${s1.eventid}',"
      j = j - 1
    }

    str2 = str2.substring(0,str2.length-1)             //去掉in后边多拼接的,


    spark.sql(
      s"""
         |
         |insert overwrite table dws.user_buy_funnel
         |partition (dt = '${dt}')
         |select deviceid,'${funnel_name}' funnel_name,
         |    case ${str}  else 0  end max_step,
         |    '${start}' funnel_starttime,
         |    '${end}' funnel_endtime
         |from(
         |    select deviceid,
         |               concat_ws(':',sort_array(collect_list(concat(`timestamp`,'_',eventid))))  c1
         |    from dwd.event_log_detail where dt >= '${start}'  and dt <=  '${end}'
         |     and  eventid in (${str2})
         |    group by deviceid
         |)t1
         |
         |""".stripMargin)

    //tmp_event_log_detail   dwd.event_log_detail
    //ts                     `timestamp`
    spark.stop()
  }
}
