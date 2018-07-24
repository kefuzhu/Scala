package com.talkingdata.dmp.etl.extract

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by chris on 7/18/17.
  */
object AppModelMerge {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("merge")
    val sc = new SparkContext(conf)
    val input = args(0)
    val output = args(1)
    val idPath = args(2)
    val start = args(3)
    val map2name = sc.broadcast(sc.textFile(args(4)).map{line =>
      (line.split(",")(0),line.split(",")(1))
    }.collect().toMap)
    val mapper = sc.textFile(idPath).map{f=>
      f.split("\t")(0)
    }.collect zip Stream.from(start.toInt)

    val id = sc.broadcast[Map[String,Int]](mapper.toMap)
    sc.makeRDD(mapper.map{case(key,value) => key + "\t" + value}).repartition(1).saveAsTextFile(output + "/schema")
    val data = sc.textFile(input).flatMap{line =>
      try{
        val tmp = line.split("\t")
        val tdid = tmp(0)
        val item = tmp(1)
        val name = map2name.value.getOrElse(item, "not found")
        val result = new ArrayBuffer[String]()
        result += name
        Some(tdid -> result)
      } catch {
        case e: Exception => None
      }
    }.reduceByKey({
      (m1,m2) => m1 ++ m2
    },48).map { case (tdid, z) =>
      val mapper = id.value
      tdid + "\t" + z.distinct.flatMap { line =>
        val index = mapper.getOrElse(line, -1)
        if (index != -1) {
          Some(s"$index:1")
        } else {
          None
        }
      }.mkString(" ")
    }.saveAsTextFile(output + "/data")
  }

}
