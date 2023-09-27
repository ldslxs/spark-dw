package lds.bigdata.ods_etl

import ch.hsr.geohash.GeoHash
import lds.bigdata.util.{AreaGeo, MyACC, SparkUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer


object AppLogToDWD {
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println("请输入时间,格式为(yyyy-mm-dd)")
      System.exit(0)
    }

    val dt = args(0)
    val spark: SparkSession = SparkUtils.getSparkSession("AppLogToDWD")
    import spark.implicits._
    val df: DataFrame = spark.read.table("dim.area_geo")
    val map: collection.Map[String, String] = df.rdd.map(row => {
      val geohash5 = row.getAs[String]("geohash5")
      val province: String = row.getAs[String]("province")
      val city = row.getAs[String]("city")
      val district = row.getAs[String]("region")
      (geohash5, province + "_" + city + "_" + district)
    }).collectAsMap()

    val bc: Broadcast[collection.Map[String, String]] =
      spark.sparkContext.broadcast(map)
    val myACC=new MyACC()
    spark.sparkContext.register(myACC);
    spark.udf.register("get_city", (latitude: Double, longitude: Double) => {
      val str: String = GeoHash.geoHashStringWithCharacterPrecision(latitude, longitude, 5)
      val map: collection.Map[String, String] = bc.value
      val option: Option[String] = map.get(str)
      if (option.isDefined) {
        option.get
      } else {
        //"未知_未知_未知"
        val str1: String = GaodeUtils.getGugeLATandLNG(latitude, longitude)
        val arr: Array[String] = str1.split("_")
        myACC.add( AreaGeo(arr(0),arr(1),arr(2),null,longitude,latitude,str))
        str1
      }
    })
    spark.sql(
      s"""
         |insert overwrite table dwd.event_log_detail
         |partition(dt='${dt}')
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
         |   where dt = '${dt}'
         | )t1
         |
         |
         |""".stripMargin).show()
    val list: ListBuffer[AreaGeo] = myACC.value
    val df2: DataFrame = list.toDF()
    df2.distinct().write.insertInto("dim.area_geo")
  }

}
