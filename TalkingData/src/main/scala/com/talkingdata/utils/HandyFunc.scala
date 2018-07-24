package com.talkingdata.utils
import java.text.SimpleDateFormat
import java.text.DateFormat
import java.text.ParseException
import java.util
import java.util.{ArrayList, Calendar, Date}

import scala.collection.mutable.ArrayBuffer
/**
  * Created by chris on 7/17/17.
  */
object HandyFunc {
  def getBeforeDate(d:Date, day:Int) = {
    val now = Calendar.getInstance()
    now.setTime(d)
    now.set(Calendar.DATE, now.get(Calendar.DATE) - day);
    now.getTime
  }
  def range(endDay:String,day:Int) = {
    val sf = new SimpleDateFormat("yyyy-MM-dd")
    val end = sf.parse(endDay)
    val begin = getBeforeDate(end,day)
    val days = new ArrayBuffer[String]()
    val c = Calendar.getInstance()
    c.setTime(begin)
    while( c.getTime().getTime() <= end.getTime() ) {
      days +=  s"${c.get(Calendar.YEAR)}/${c.get(Calendar.MONTH) + 1}/${c.get(Calendar.DAY_OF_MONTH)}"
      c.add(Calendar.DAY_OF_YEAR, 1)
    }
    days.toArray
  }
  def format(tdid:String) = tdid.toLowerCase().replaceAll("[^0-9a-z]","")

  def main(args: Array[String]): Unit = {
    range("2017-02-10",10).foreach(println(_))

  }
}
