package com.talkingdata.dmp.etl.extract

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import scala.collection.mutable

/**
  * Created by chris on 6/21/17.
  */
object AoiExtract2nd {
  def main(args:Array[String]):Unit = {
    val input = args(0)
    val output = args(1)
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val data = sqlContext.read.json(input).flatMap{row =>
      try {
        val fields = mutable.Map[String, Int]()
        val tdid = row.getAs[String]("tdid")
        val aoiName = row.getAs[String]("aoiName")
        val aoiLabel = row.getAs[String]("aoiLabel")
        val key = s"${aoiLabel}|${aoiName}"
        if (aoiName!= "null" && aoiName != null && !aoiName.isEmpty) fields += key -> 1
        Some(tdid -> fields)
      }catch {
        case e: Exception => None
      }
    }.reduceByKey({
      (m1,m2) => m2.foreach{
        case (k,v2) => m1.get(k) match {
          case Some(v) => m1 += k -> (v + v2)
          case None => m1 += k -> v2
        }
      }
        m1
    },24).map{
      case (tdid,fields) => tdid + "\t" + fields.map{case (k,v) => s"$k:$v"}.mkString(",")
    }.saveAsTextFile(output)
  }
}
