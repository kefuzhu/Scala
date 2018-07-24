package com.talkingdata.dmp.etl.extract

import java.util.Calendar

import com.talkingdata.dmp.util.DateUtil
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by takun on 2017/7/4.
  */
object AoiHourExtract {

  val location = "/data/datacenter/solar-system/Location/"
  val label = "/user/yihong.zhang/guozhengtong/trueGenderAgeAll/cb_gzt_union_gender_u"
  def format(tdid:String) = tdid.toLowerCase().replaceAll("[^0-9a-z]", "")

  def main(args: Array[String]): Unit = {
    // for spark 2.x
//    val session = SparkSession.builder().getOrCreate()
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val session = new SQLContext(sc)
    val date = args(0)
    val wd = DateUtil.format(date, "weekday")
    val date2 = DateUtil.format(date, "yyyy/MM/dd")
    val pn = 48
    val tdid_label = session.sparkContext.textFile(label).map{
      line =>
        val spl = line.split(",")
        format(spl(0)) -> spl(1).toInt
    }.reduceByKey(_|_, pn)
    val aois = session.read.parquet(location + wd).select("tdid", "aoiName", "time").rdd.map{
      row =>
        val time = row.getLong(2)
        val c = Calendar.getInstance()
        c.setTimeInMillis(time)
        val hour = c.get(Calendar.HOUR_OF_DAY)
        format(row.getString(0)) -> (hour, row.getString(1))
    }.filter(v => v._2._2 != null && v._2._2 != "null").map{
      case (tdid, aoi) => tdid -> Set(aoi)
    }.reduceByKey(_++_, pn).filter(_._2.nonEmpty)
    aois.zipPartitions(tdid_label){
      (it1, it2) =>
        val map = it2.toMap
        it1.flatMap{
          case (tdid, _aois) =>
            map.get(tdid) match {
              case None => Nil
              case Some(_label) => List((tdid, (_label,_aois)))
            }
        }
    }.map{
      case (tdid, (_label, _aois)) =>
        tdid + "\t" + _label + "\t" + _aois.map{
          case (k,v) => k + ":" + v
        }.mkString(",")
    }.saveAsTextFile("takun/data/sex/aois/" + date2)
  }
}
