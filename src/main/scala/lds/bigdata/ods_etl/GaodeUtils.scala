package lds.bigdata.ods_etl

import cn.hutool.json.{JSONObject, JSONUtil}
import scalaj.http.Http

object GaodeUtils {
  def getGugeLATandLNG(latitude:Double,longitude:Double): String ={
    val jsonStr = Http("https://restapi.amap.com/v3/geocode/regeo?")
      .param("key", "31ad3964171bc3f85a3d3a1e45a052a6")
      .param("location", s"${longitude},${latitude}")
      .asString.body

    println(s" 高德逆地理位置接口返回的json ${jsonStr}")
    if(jsonStr==null||"".equals(jsonStr)){
       "未知_未知_未知"
    }else{
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
        "未知_未知_未知"
      }
    }

  }

}
