package com.talkingdata.dmp.etl.extractlarge

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.talkingdata.dmp.etl.feature.Hash
import com.talkingdata.utils.{HandyFunc, PathUtil}
import com.talkingdata.utils.json.{array, string}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by chris on 7/24/17.
  */
object ActivityMergeHash {

  def dayReport(month:Date, days:Int) = {
    val cal = Calendar.getInstance()
    cal.setTime(month)
    val year = cal.get(Calendar.YEAR)
    val m = cal.get(Calendar.MONTH) + 1
    cal.set(Calendar.DAY_OF_MONTH,0)
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    cal.add(Calendar.DATE, days)
    for(i<- 0 until days) yield{
      cal.add(Calendar.DATE,-1)
      sdf.format(cal.getTime)
    }
  }


  def parse(line: String) = {
    val root = json4s.jackson.parseJson(line)
    val tdid = string(root \\ "tdid")
    val pkg = string(root \\ "appKey")
    val activities = array(root \\ "activities")
    val record = new ArrayBuffer[(String, mutable.Buffer[(String,String)])]()
    val z = activities.map { e => (string(e \ "name"), string(e \ "refer")) }.distinct.toBuffer
    record += ((pkg, z))
    (tdid, record)
  }
  def main(args: Array[String]): Unit = {
    val input = args(0)
    val output = args(1)
    val pkgMap = args(2)
    val start = args(3).toInt
    val monthBegin = args(4)
    val days = args(5).toInt
    val sdf = new SimpleDateFormat("yyyy-MM")
    val date = sdf.parse(monthBegin)
    val conf = new SparkConf().setAppName("ActivityMergeAndHash")
    val sc = new SparkContext(conf)
    val inputFinal = dayReport(date,days).map{f=>
      s"$input/$f"
    }.filter(f=>PathUtil.isExisted(sc,f)).mkString(",")

    if(PathUtil.isExisted(sc,output)){
      PathUtil.deleteExistedPath(sc, output)
    }

    val mapper2weight = sc.broadcast(sc.textFile(pkgMap).map{
      line =>
        (line.split("\t")(0),line.split("\t")(1).toFloat)
    }.collect.toMap)

    val data = sc.textFile(inputFinal).flatMap { line =>
      try{
        val r@(tdid, z) = parse(line)
        Some(r._1 -> r._2)
      } catch {
        case e: Exception => None
      }
    }.reduceByKey(
      { (m1, m2) => (m1 ++ m2).slice(0, 20)
      }
      , 1000).map { case (tdid, z) =>
        val mapper = mapper2weight.value
        val tdidFormated = HandyFunc.format(tdid)
        val feature = z.flatMap{case (pkg, activities) =>
          val pkgStriped = pkg.stripPrefix("\"").stripSuffix("\"")
          activities.flatMap{case (name,refer) =>
            Array((name,mapper.getOrElse(pkgStriped,0f)),(refer,mapper.getOrElse(pkgStriped,0f)))
          }.distinct
        }
        val hash = Hash.SimHash(feature.toArray,256,Hash.Fnv1aHash)
        val str = (hash.split("").filter(_.nonEmpty).map(_.toInt) zip Stream.from(start)).flatMap{case(feature, index)=>
          if(feature != 0){
            Some(s"$index:1")
          } else {
            None
          }
        }.mkString(" ")
        s"$tdidFormated $str"
    }.saveAsTextFile(output)
  }
}
