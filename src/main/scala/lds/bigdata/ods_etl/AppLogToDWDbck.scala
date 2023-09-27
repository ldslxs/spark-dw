package lds.bigdata.ods_etl

import cn.hutool.json.{JSONObject, JSONUtil}
import lds.bigdata.util.SparkUtils
import org.apache.spark.sql.SparkSession
import scalaj.http.Http

object AppLogToDWDbck {
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println("请输入时间,格式为(yyyy-mm-dd)")
      System.exit(0)
    }
    val dt = args(0)
    val spark: SparkSession = SparkUtils.getSparkSession("AppLogToDWD")

    spark.udf.register("get_city", (lat: Double, long: Double) => { //参数1：纬度 参数2：经度
      val jsonStr = Http("https://restapi.amap.com/v3/geocode/regeo")
        .param("key", "70c317bce322be925d9b88ba094c119c")
        .param("location", s"${long},${lat}")
        .asString.body

      println(s" 高德逆地理位置接口返回的json ${jsonStr}")
      val jSONObject: JSONObject = JSONUtil.parseObj(jsonStr)
      val infoCode: String = jSONObject.getStr("infocode")
      if ("10000".equals(infoCode)) {
        val jSONObject1: JSONObject = jSONObject.getJSONObject("regeocode").getJSONObject("addressComponent")
        val province: String = jSONObject1.getStr("province")
        val city: String = jSONObject1.getStr("city")
        val district: String = jSONObject1.getStr("district")
        //println(province + " " + city + " " + district)
        province + "_" + city + "_" + district
      } else {
        "null_null_null"
      }
    })
    spark.sql(
      s"""
         |insert overwrite table dwd.event_log_detail
         |partition(dt='2023-09-07')
         | select
         |   account        ,
         |   appid          ,
         |   appversion     ,
         |   carrier        ,
         |   deviceid       ,
         |   devicetype     ,
         |   eventid        ,
         |   ip             ,
         |   latitude       ,
         |   longitude      ,
         |   nettype        ,
         |   osname         ,
         |   osversion      ,
         |   properties     ,
         |   releasechannel ,
         |   resolution     ,
         |   sessionid      ,
         |   `timestamp`    ,
         |   newsessionid   ,
         |   split(x1,'_')[0]    province ,
         |   split(x1,'_')[1]    city     ,
         |   split(x1,'_')[2]    district
         | from(
         |    select *,get_city(round(latitude,6),round(longitude,6)) x1
         |    from tmp.event_log_splited
         |   where dt = '${dt}' and account = 'rb,ks'
         | )t1
         |
         |
         |""".stripMargin).show()
  }

}
