package com.talkingdata.dmp.etl.extractlarge

import com.talkingdata.utils.PathUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by chris on 7/25/17.
  */
object Aoi {
  def parse(line:String) = {
    try{
      val tmp = line.split("\t")
      Some(tmp(0), tmp(2).split(",").map(_.split(":")(0)))
    } catch {
      case e:Exception => None
    }
  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val input = args(0)
    val output = args(1)
    val entrySetPath = args(2)


    val mapper = sc.broadcast(sc.textFile(entrySetPath).flatMap(line => {
      try {
        val tmp = line.split(",")
        Some((tmp(0).stripMargin, Integer.parseInt(tmp(1))))
      } catch {
        case e: Exception => None
      }
    }).collect.toMap)

    if(PathUtil.isExisted(sc,output)){
      PathUtil.deleteExistedPath(sc, output)
    }


    val data = sc.textFile(input).flatMap{f => parse(f)}.flatMap{r =>
      val map = mapper.value
      val feature = r._2.flatMap{f =>
        val a = map.getOrElse(f, 0)
        if(a != 0) {
          Some(s"$a:1")
        } else {
          None
        }
      }
      if(feature.nonEmpty){
        Some(s"${r._1} ${feature.mkString(" ")}")
      } else {
        None
      }
    }.repartition(1000).saveAsTextFile(output)
  }
}
