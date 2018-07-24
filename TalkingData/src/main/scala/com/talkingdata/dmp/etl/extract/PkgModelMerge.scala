package com.talkingdata.dmp.etl.extract

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by chris on 7/17/17.
  */
object PkgModelMerge {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("merge")
    val sc = new SparkContext(conf)
    val input = args(0)
    val output = args(1)
    val idPath = args(2)
    val start = args(3)
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
        val result = new ArrayBuffer[String]()
        result += item
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
