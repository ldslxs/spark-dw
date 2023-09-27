package lds.bigdata.ods_etl


import lds.bigdata.util.SparkUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object App {
  def main(args: Array[String]): Unit = {


    val spark: SparkSession = SparkUtils.getSparkSession("AppLogToDWD")
    val sc: SparkContext = spark.sparkContext
    val rdd: RDD[Int] = sc.makeRDD(1 to 10,1)
    var sum=0
    rdd.map(v=>{
      sum+=v
    })
    println(s"sum = ${sum}")



  }

}
