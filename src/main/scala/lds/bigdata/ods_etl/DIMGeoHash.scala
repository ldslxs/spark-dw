package lds.bigdata.ods_etl

import ch.hsr.geohash.GeoHash
import com.typesafe.config.{Config, ConfigFactory}
import lds.bigdata.util.{ConfigUtils, SparkUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.{Driver, DriverManager}
import java.util.Properties


object DIMGeoHash {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkUtils.getSparkSession("DIM")
    val config: Config = ConfigFactory.load()

    var prop=new  Properties()
    prop.setProperty("driver",ConfigUtils.driverClass)
    prop.setProperty("user","root")

    prop.setProperty("password","123456")
     val df: DataFrame = spark.read.jdbc(ConfigUtils.url, "t_md_areas", prop)
    df.createTempView("table1")
    spark.udf.register("geohash",(latitude :Double,longitude:Double)=>{
      val str: String = GeoHash.geoHashStringWithCharacterPrecision(latitude, longitude, 5)
      str
    })
    val ds2: DataFrame = spark.sql(
      """

        |select t1.AREANAME as province,t2.AREANAME as city,t3.AREANAME as region,t4.AREANAME as street ,
        | t4.BD09_LNG longitude, t4.BD09_LAT latitude,geohash(round(t4.BD09_LAT,6),round(t4.BD09_LNG,6)) geohash5
        |from table1 t1 join table1 t2 on t1.id=t2.PARENTID and t1.PARENTID=0
        |join table1 t3 on t2.ID=t3.PARENTID join table1 t4
        |on t4.PARENTID=t3.ID
        |""".stripMargin)
        ds2.write.saveAsTable("dim.area_geo")


    spark.stop()
  }

}
