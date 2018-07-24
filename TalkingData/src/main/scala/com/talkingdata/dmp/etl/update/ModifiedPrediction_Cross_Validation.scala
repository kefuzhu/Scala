package com.talkingdata.dmp.etl.update

import com.talkingdata.utils.PathUtil
import org.apache.spark.rdd.UnionRDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  *
  * Modified by kefu on 5/3/18.
  */
object ModifiedPrediction_Cross_Validation {
  def main(args: Array[String]): Unit = {
    val input = args(0).split(",")
    val outputMale = args(1)
    val outputFemale = args(2)
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val partition = args(3).toInt

    val maleTagID = "304"
    val femaleTagID = "305"

    if(PathUtil.isExisted(sc,outputMale)){
      PathUtil.deleteExistedPath(sc, outputMale)
    }

    if(PathUtil.isExisted(sc,outputFemale)){
      PathUtil.deleteExistedPath(sc, outputFemale)
    }

    val inputNow = input.filter(PathUtil.isExisted(sc, _))


    val data = new UnionRDD(sc,inputNow.map{path =>
      sc.textFile(path).flatMap{line =>
        try{
          val col = line.split("\t")
          val tdid = col(0)
          val label = col(1)
          val pred = ArrayBuffer(col(2).toFloat)
          Some((label,tdid) -> pred)
        } catch {
          case e:Exception => None
        }
      }
    }).reduceByKey({
      (m1,m2) => m1 ++ m2
    },partition).flatMap{case ((label,tdid), feature)=>
      try{
        val max = feature.max
        val min = feature.min
        Some((label,tdid,max,min))
      } catch {
        case e:Exception => None
      }
    }

    data.filter(f =>f._3 >= 0.65 && f._4 > 0.40).sortBy(-_._3).map(r=>s"${r._1}\t${r._2}\t$maleTagID\t${100*r._3}").coalesce(512).saveAsTextFile(outputMale)
    data.filter(_._4 <= 0.40).sortBy(f=> f._4).map(r=>s"${r._1}\t${r._2}\t$femaleTagID\t${100*(1 - r._4)}").coalesce(512).saveAsTextFile(outputFemale)
  }
}
