package lds.bigdata.util

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable.ListBuffer

class MyACC extends AccumulatorV2[AreaGeo, ListBuffer[AreaGeo]]{
  val list = new ListBuffer[AreaGeo]()
  override def isZero: Boolean = list.isEmpty
  override def copy(): AccumulatorV2[AreaGeo, ListBuffer[AreaGeo]] = {
    new MyACC
  }

  override def reset(): Unit = {
    list.clear()
  }

  override def add(v: AreaGeo): Unit = {
    list.append(v)
  }

  override def merge(other: AccumulatorV2[AreaGeo, ListBuffer[AreaGeo]]): Unit = {
    list.++=(other.value)

  }

  override def value: ListBuffer[AreaGeo] = list
}
