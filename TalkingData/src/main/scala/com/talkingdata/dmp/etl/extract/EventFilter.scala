package com.talkingdata.dmp.etl.extract

import com.talkingdata.utils.json.{array, string}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s

/**
  * Created by chris on 7/6/17.
  */
object EventFilter {

  def parse(line:String) = {
    val root = json4s.jackson.parseJson(line)
    val tdid = string(root \\ "tdid")
    val appKey = string(root \\ "appkey")
    val events = array(root \\ "events")
    val z = events.map{e => string(e \ "id")}.distinct.mkString(" ")
    (tdid, appKey, z)
  }

  def apply(line:String, mapper:Map[String,Int]) = {
    try{
      val (tdid,appKey,z) = parse(line)
      val label = mapper.getOrElse(tdid, null)
      if (label != null) {
        Some(s"$tdid $appKey $z $label")
      } else {
        None
      }
    } catch {
      case e:Exception => None
    }
  }
  def main(args: Array[String]): Unit = {
    val input = args(0)
    val output = args(1)
    val id = args(2)
    val conf = new SparkConf().setAppName("EventFilter")
    val sc = new SparkContext(conf)
    val mapper = sc.textFile(id).flatMap(line => {
      try{
        val tmp = line.split(",")
        Some((tmp(0).stripMargin,Integer.parseInt(tmp(1))))
      }catch{
        case e:Exception => None
      }
    }).collect.toMap

    val broadcastMapper = sc.broadcast[Map[String,Int]](mapper)
    val data = sc.textFile(input).flatMap{line =>
      val mapper = broadcastMapper.value
      apply(line, mapper)
    }.repartition(24).saveAsTextFile(output)
  }
}
