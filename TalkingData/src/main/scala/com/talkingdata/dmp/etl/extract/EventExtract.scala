package com.talkingdata.dmp.etl.extract
import org.json4s

import scala.io.Source
import java.io.File

import com.talkingdata.utils.json._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by chris on 7/5/17.
  */



object EventExtract {
  def apply(line:String) = {
    try{
      val root = json4s.jackson.parseJson(line)
      val tdid = string(root \\ "tdid")
      val appKey = string(root \\ "appkey")
      val events = array(root \\ "events")
      val z = events.map{e => string(e \ "id")
      }.distinct.mkString(" ")
      Some(s"$tdid $appKey $z")
    } catch {
      case e:Exception => None
    }
  }

  def main(args: Array[String]): Unit = {
    val input = args(0)
    val output = args(1)
    val conf = new SparkConf().setAppName("EventParse")
    val sc = new SparkContext(conf)
    sc.textFile(input).flatMap{line =>
      apply(line)
    }.saveAsTextFile(args(1))
  }
}
