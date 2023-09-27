package lds.bigdata.analysis

import lds.bigdata.util.{ConfigUtils, SparkUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.mutable.ListBuffer




object AttributionAnalysis  {
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println("请输入时间,格式为(yyyy-mm-dd)")
      System.exit(0)
    }

    val dt = args(0)
    val spark: SparkSession = SparkUtils.getSparkSession("AppLogToDWD")
    import spark.implicits._
    spark.sql("select * from tmp_event_log_detail").createTempView("t1")
    val df: DataFrame = spark.sql("select * from t1 where " +
      "eventid in ('提交订单','APP激活','坑位曝光')").toDF()
    val rdd1: RDD[(String, AttributionBean)] = df.rdd.map(row => {
      val deviceid = row.getAs[String]("deviceid")
      val eventid = row.getAs[String]("eventid")
      val ts = row.getAs[Long]("ts")
      (deviceid, AttributionBean(deviceid, eventid, ts))
    })
    val rdd2: RDD[(String, Iterable[AttributionBean])] = rdd1.groupByKey()
    val list1 = new ListBuffer[ListBuffer[AttributionBean]]
    var list2 = new ListBuffer[AttributionBean]
    val rdd3: RDD[(String, String, String, String, String, Int)] = rdd2.flatMap(v => {
      val deviceid: String = v._1
      val list: List[AttributionBean] = v._2.toList.sortBy(v => v.ts)
      for (bean <- list) {
        if (!bean.eventid.equals("提交订单")) { //待归因事件
          list2.append(bean)
        } else {
          list1.append(list2)
          list2 = new ListBuffer[AttributionBean]
        }
      }
      list1.map(v => {
        ("提交订单事件归因分析", "首次触点归因分析", deviceid, "提交订单", v.head.eventid, 1)
      })
    })
    rdd3.toDF().write.insertInto("dws.attribution_analysis")
    spark.stop()
  }
}
