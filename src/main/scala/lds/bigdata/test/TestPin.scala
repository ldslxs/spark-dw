package lds.bigdata.test

import lds.bigdata.util.SparkUtils
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

object TestPin {
  def main(args: Array[String]): Unit = {
//    if (args.length == 0) {
//      println("请传递一个日期参数")
//      System.exit(0) //退出虚拟机，说白了就是不执行后续代码
//    }
//    val dt = args(0)
//    val spark: SparkSession = SparkUtils.getSparkSession("ToAdsUserActiveDayRange")
//    spark.stop()
    val list: ListBuffer[Funnel] = ListBuffer()
    list.append(Funnel(1, "测试1", 1, "display", "浏览商品"))
    list.append(Funnel(2, "测试1", 2, "addCart", "添加购物车"))
    list.append(Funnel(3, "测试1", 3, "order", "下订单"))
    list.append(Funnel(4, "测试1", 4, "pay", "支付"))
    var str=""
    var str2="("
    var j=list.size
    for (i <- 0 until  list.size){
      var str1=""
      str2+=s"'${list(i).eventid}',"
      for (k<- 0 until j ){
        str1+=s".*(${list(k).eventid}):"
      }
      str1=str1.substring(0,str1.length-1)
      str+=s"when regexp_extract(c1,'${str1}',${j})='${list(j-1).eventid}' then ${j}\n"
      j-=1
    }
    str2=str2.substring(0,str2.length-1)+")"

    println(str)
    println(str2)
    var s=  s"""
         |insert overwrite table dws.user_buy_funnel partition(dt='2022-11-25')
         |select deviceid,'测试1' funnel_name,
         |    case
         |          ${str} end max_step,
         |    '2022-11-25' funnel_starttime,'2022-11-25' funnel_endtime
         |      from (select deviceid, concat_ws(':', sort_array(collect_list(concat(ts, '_', eventid)))) c1
         |      from tmp_event_log_detail
         |      where eventid in ${str2}
         |        and dt >= '2022-11-25'  and dt <=  '2022-11-25'
         |      group by deviceid) t1
         |""".stripMargin

    println(s)
  }
}
