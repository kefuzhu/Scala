package com.talkingdata.dmp.etl.extract

import com.talkingdata.utils.HandyFunc
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by chris on 7/18/17.
  */
object DeviceTag {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("merge")
    val sc = new SparkContext(conf)
    val input = args(0)
    val output = args(1)
    val idPath = args(2)
    val start = args(3)
    val info = sc.textFile(idPath)
    val mapperInfo= (info.flatMap{f=>
      f.split("\t")(1).split(" ")
    }.collect.distinct zip Stream.from(start.toInt)).toMap

    val infoMap = sc.broadcast(info.map{ line =>
      val device = line.split("\t")(0)
      val feature = line.split("\t")(1).split(" ").map(mapperInfo.getOrElse(_,0))
      (device, feature)
    }.collect.toMap[String,Array[Int]])

    sc.makeRDD(mapperInfo.map{case(key,value) => key + "\t" + value}.toSeq).repartition(1).saveAsTextFile(output + "/schema")
    val data = sc.textFile(input).flatMap{line =>
      try{
        val tmp = line.split("\t")
        val tdid = HandyFunc.format(tmp(0))
        val item = infoMap.value.getOrElse(tmp(1),Array[Int]())
        if(item.nonEmpty){
          val str = tdid+"\t"+item.map{f=>
            s"$f:1"
          }.mkString(" ")
          Some(str)
        } else {
          None
        }
      } catch {
        case e: Exception => None
      }
    }.saveAsTextFile(output + "/data")
  }

}
