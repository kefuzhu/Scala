package com.talkingdata.dmp.etl.extract

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable

/**
  * Created by chris on 6/20/17.
  */
object AoiExtract {
  def main(args:Array[String]):Unit = {
    val input = args(0)
    val output = args(1)
    val aoi = args(2) match {
      case "label" => "aoiLabel"
      case "name" => "aoiName"
      case "pkg" => "pkg"
    }
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val data = sqlContext.read.json(input).flatMap{row =>
      try {
        val fields = mutable.Map[String, Int]()
        val tdid = row.getAs[String]("tdid")
        val aoiExtract = row.getAs[String](aoi)
        if (aoiExtract != "null" && aoiExtract != null && !aoiExtract.isEmpty) fields += aoiExtract -> 1
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
