package com.talkingdata.dmp.etl.update

import com.talkingdata.utils.PathUtil
import org.apache.spark.rdd.UnionRDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by kefu on 03/02/18.
  *
  * Based on ModifiedPrediction.scala
  */
object ModifiedPrediction_batch_output {
  def main(args: Array[String]): Unit = {
    val input = args(0).split(",")
    val outputMale = args(1)
    val outputFemale = args(2)
    val outputSample = args(3)
    val outputMaleTmp = args(4)
    val outputFemaleTmp = args(5)
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val batchNum = args(6).toInt
    val partition = args(7).toInt

    // Initialize seed number
    val seedNum = 12345

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

    // Combine results from all sub-models
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

    // Output data in batches
    val batches = data.randomSplit(List.fill(batchNum)(1.0/batchNum).toArray, seed = seedNum)

    // Initialize counter
    var counter = 1
    var female_batch_output = ""
    var male_batch_output = ""

    for(i <- batches){

      val tmp_female_out = outputFemaleTmp + "_" + counter
      val tmp_male_out = outputMaleTmp + "_" + counter

      female_batch_output = female_batch_output + tmp_female_out + " "
      male_batch_output = male_batch_output + tmp_male_out + " "

      if(PathUtil.isExisted(sc,tmp_male_out)){
        PathUtil.deleteExistedPath(sc, tmp_male_out)
      }

      if(PathUtil.isExisted(sc,tmp_female_out)){
        PathUtil.deleteExistedPath(sc, tmp_female_out)
      }
      // Write out batch #i
      i.filter(f =>f._2 >= 0.65 && f._3 > 0.40).sortBy(-_._2).map(r=>s"${r._1}\t$maleTagID\t${100*r._2}").coalesce(512).saveAsTextFile(tmp_male_out)
      i.filter(_._3 <= 0.40).sortBy(f=> f._3).map(r=>s"${r._1}\t$femaleTagID\t${100*(1 - r._3)}").coalesce(512).saveAsTextFile(tmp_female_out)
      // Increment the counter
      counter += 1
    }

    // Combine all batches for male and female
    val male = new UnionRDD(sc,male_batch_output.split(" ").map{path =>
      sc.textFile(path)})
    val female = new UnionRDD(sc,female_batch_output.split(" ").map{path =>
      sc.textFile(path)})

    // Write out combined result
    male.coalesce(512).saveAsTextFile(outputMale)
    female.coalesce(512).saveAsTextFile(outputFemale)
  }
}
