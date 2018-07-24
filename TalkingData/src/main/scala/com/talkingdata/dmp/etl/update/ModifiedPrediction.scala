package com.talkingdata.dmp.etl.update

import com.talkingdata.utils.PathUtil
import org.apache.spark.rdd.UnionRDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by chris on 7/27/17.
  *
  * Modified by kefu on 10/16/17.
  *  1. Female & Male Output changed
  *   Before: TDID  PredictionScore (In scale of 0.6-1)
  *   Now: TDID  TagID  PredictionScore (In scale of 60-100)
  */
object ModifiedPrediction {
  def main(args: Array[String]): Unit = {
    val input = args(0).split(",")
    val outputMale = args(1)
    val outputFemale = args(2)
    val outputSample = args(3)
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val partition = args(4).toInt

    val maleTagID = "304"
    val femaleTagID = "305"

    if(PathUtil.isExisted(sc,outputMale)){
      PathUtil.deleteExistedPath(sc, outputMale)
    }

    if(PathUtil.isExisted(sc,outputFemale)){
      PathUtil.deleteExistedPath(sc, outputFemale)
    }

    if(PathUtil.isExisted(sc,outputSample)){
      PathUtil.deleteExistedPath(sc, outputSample)
    }

    val inputNow = input.filter(PathUtil.isExisted(sc, _))


    val data = new UnionRDD(sc,inputNow.map{path =>
      sc.textFile(path).flatMap{line =>
        try{
          Some(line.split("\t")(0) -> ArrayBuffer(line.split("\t")(1).toFloat))
        } catch {
          case e:Exception => None
        }
      }
    }).reduceByKey({
      (m1,m2) => m1 ++ m2
    },partition).flatMap{case (tdid, feature)=>
      try{
        val max = feature.max
        val min = feature.min
        Some((tdid,max,min))
      } catch {
        case e:Exception => None
      }
    }


    data.filter(f =>f._2 >= 0.65 && f._3 > 0.40).sortBy(-_._2).map(r=>s"${r._1}\t$maleTagID\t${100*r._2}").coalesce(512).saveAsTextFile(outputMale)
    data.filter(_._3 <= 0.40).sortBy(f=> f._3).map(r=>s"${r._1}\t$femaleTagID\t${100*(1 - r._3)}").coalesce(512).saveAsTextFile(outputFemale)
    data.flatMap{f =>
      if(f._2 >= 0.95) {
        Some(s"${f._1},1")
      } else if(f._3 <= 0.15){
        Some(s"${f._1},0")
      } else {
        None
      }
    }.coalesce(512).saveAsTextFile(outputSample)
  }
}
