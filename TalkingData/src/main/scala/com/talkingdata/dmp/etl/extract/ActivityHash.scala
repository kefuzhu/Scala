package com.talkingdata.dmp.etl.extract

import com.talkingdata.utils.HandyFunc
import com.talkingdata.utils.json.{array, int, long, string}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import org.json4s._
import com.talkingdata.dmp.etl.feature.Hash
/**
  * Created by chris on 7/18/17.
  */
object ActivityHash {
  def parse(line: String) = {
    val root = json4s.jackson.parseJson(line)
    val tdid = string(root \\ "tdid")
    val pkg = string(root \\ "appKey")
    val time = long(root \\ "time")
    val activities = array(root \\ "activities")
    val record = new ArrayBuffer[(String, Long, mutable.Buffer[(String, Long, Int, String)])]()
    val z = activities.map { e => (string(e \ "name"), long(e \ "start"), int(e \ "duration"), string(e \ "refer")) }.distinct.toBuffer
    record += ((pkg, time, z))
    (tdid, record)
  }
  def main(args: Array[String]): Unit = {
    val input = args(0)
    val output = args(1)
    val id = args(2)
    val pkgMap = args(3)
    val start = args(4).toInt
    val conf = new SparkConf().setAppName("ActivityFilter")
    val sc = new SparkContext(conf)
    val mapper = sc.textFile(id).flatMap(line => {
      try {
        val tmp = line.split(",")
        Some((tmp(0).stripMargin, Integer.parseInt(tmp(1))))
      } catch {
        case e: Exception => None
      }
    }).collect.toMap
    val mapper2weight = sc.broadcast(sc.textFile(pkgMap).map{
      line =>
      (line.split("\t")(0),line.split("\t")(1).toFloat)
    }.collect.toMap)

    val broadcastMapper = sc.broadcast[Map[String, Int]](mapper)
    val data = sc.textFile(input).flatMap { line =>
      try {
        val mapper = broadcastMapper.value
        val r@(tdid, z) = parse(line)
        val label = mapper.getOrElse(r._1, 2)
        if (label != 2) {
          Some(r._1 -> (label, r._2))
        } else {
          None
        }
      } catch {
        case e: Exception => None
      }
    }.reduceByKey(
      { (m1, m2) => (m1._1, (m1._2 ++ m2._2).slice(0, 20))
      }
      , 48).flatMap { case (tdid, (label, z)) =>
      try {
        val mapper = mapper2weight.value
        val tdidFormated = HandyFunc.format(tdid)
        val feature = z.flatMap{case (pkg, time, activities) =>
            val pkgStriped = pkg.stripPrefix("\"").stripSuffix("\"")
            activities.flatMap{case (name, start, duration, refer) =>
              Array((name,mapper.getOrElse(pkgStriped,0f)),(refer,mapper.getOrElse(pkgStriped,0f)))
            }.distinct
        }
        val hash = Hash.SimHash(feature.toArray,128,Hash.Fnv1aHash)
        val str = (hash.toArray zip Stream.from(start)).flatMap{case(feature, index)=>
            if(feature.toInt != 0){
              Some(s"$index:1")
            } else {
              None
            }
        }.mkString(" ")
        Some(s"$tdidFormated\t$str")
      } catch {
        case e: Exception => None
      }
    }.saveAsTextFile(output)
  }
}
