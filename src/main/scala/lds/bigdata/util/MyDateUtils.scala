package lds.bigdata.util

import org.apache.commons.lang.time.DateFormatUtils
import org.apache.commons.lang3.time.DateUtils

import java.text.DateFormat
import java.util.Date


object MyDateUtils {
  def getYesterday(dt: String) = {
    val date: Date = DateUtils.parseDate(dt, "yyyy-MM-dd")
    val str: String = DateFormatUtils.format(DateUtils.addDays(date, -1), "yyyy-MM-dd")
    str
  }
}
