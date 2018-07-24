package com.talkingdata.dmp.etl.extract

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by chris on 6/26/17.
  */
object IdFilter {
  def main(args:Array[String]):Unit = {
    val conf = new SparkConf().setAppName("filter")
    val sc = new SparkContext(conf)
    val id =  sc.textFile(args(0)).collect.toSet
    val broadcastId = sc.broadcast(id)
    val data = sc.textFile(args(1)).map{f=>
      f.split("\t")
    }.filter{f =>
      broadcastId.value.contains(f(0))
    }.flatMap{f =>
      try{
        Some(s"${f(0)}\t${f(1)}")
      } catch {
        case e:Exception => None
      }
    }.saveAsTextFile(args(2))
  }
}
