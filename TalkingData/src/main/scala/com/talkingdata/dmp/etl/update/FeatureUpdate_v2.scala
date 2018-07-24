package com.talkingdata.dmp.etl.update

import com.talkingdata.dmp.etl.feature.Entropy
import com.talkingdata.utils.PathUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by kefu on 01/08/2018.
  */
object FeatureUpdate_v2 {
  def main(args: Array[String]): Unit = {
    val inputs = args(0).split(",")
    val outputEntropys = args(1).split(",")
    val outputRanks = args(2).split(",")
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    inputs zip outputEntropys zip outputRanks foreach {
      case((input, outputEntropy),outputRank) =>
        if(PathUtil.isExisted(sc, outputEntropy)){
          PathUtil.deleteExistedPath(sc,outputEntropy)
        }
        if(PathUtil.isExisted(sc,outputRank)){
          PathUtil.deleteExistedPath(sc,outputRank)
        }
        val data = sc.textFile(input)
        val sampleMap = data.map {f =>
          f.split("\t")(2)
        }.countByValue()
        val pos = sampleMap.getOrElse("1", 3l)
        val neg = sampleMap.getOrElse("0", 3l)

        //Calculater entropy
        val entropy = Entropy.Entropy(data,pos,neg)
        entropy.map{f=>
          s"${f._1}\t${f._2}\t${f._3}"
        }.saveAsTextFile(outputEntropy)

        //Calculate rank based on entropy
        entropy.map{
          x => (x._1,x._2)
        }.groupBy{
          x => x._2
        }.sortBy(x=>x._1).zipWithIndex().map{
          case(value,index) =>
            value._2.map{
              x => (index,x._1)
            }
        }.flatMap{
          f => f
        }.map{case(rank, name) =>
          s"$name,${rank+1}"
        }.saveAsTextFile(outputRank)
    }

  }
}
