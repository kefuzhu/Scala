package com.talkingdata.dmp.etl.extract

import org.apache.spark.{SparkConf,SparkContext}
import scala.collection.mutable
/**
  * Created by chris on 6/23/17.
  */
object AppMerge {
  def apply(f:String) = {
    try {
      val line = f.split("\t",2)
      val tdid = line(0)
      val tmp = line(1).split(",",-1)
      val fields = mutable.Map[String,Int]()
      var tmp2 = Array[String]()
      (0 until tmp.length).foreach{
        i =>
          tmp2 = tmp(i).split(":",2)
          fields += tmp2(0) -> Integer.parseInt(tmp2(1))
      }
      Some(tdid -> fields)
    } catch {
      case e :Exception => None
    }
  }

  def main(args: Array[String]): Unit = {
    val input = args(0)
    val output = args(1)
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val data = sc.textFile(input).flatMap{
      f =>
        apply(f)
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
