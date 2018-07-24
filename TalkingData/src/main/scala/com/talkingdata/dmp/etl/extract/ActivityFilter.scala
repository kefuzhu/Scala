package com.talkingdata.dmp.etl.extract
import com.talkingdata.utils.json.{array, int, long, string}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

/**
  * Created by chris on 7/7/17.
  */
object ActivityFilter {
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
      , 24).flatMap { case (tdid, (label, z)) =>
      try {
        val json = ("tdid" -> tdid) ~ ("label" -> label) ~ ("activityFeature" -> z.map { case (pkg, time, activities) =>
          ("pkg" -> pkg.stripPrefix("\"").stripSuffix("\"")) ~ ("receiveTime" -> time) ~ ("activities" -> activities.map { case (name, start, duration, refer) =>
            ("name" -> name.stripPrefix("\"").stripSuffix("\"")) ~ ("start" -> start) ~ ("duration" -> duration) ~ ("refer" -> refer.stripPrefix("\"").stripSuffix("\""))
          }
            )
        }
          )
        Some(compact(render(json)))
      } catch {
        case e: Exception => None
      }
    }.saveAsTextFile(output)
  }
}
