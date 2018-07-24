package com.talkingdata.dmp.etl.extractlarge

import com.talkingdata.utils.PathUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by chris on 7/26/17.
  */
object TrainsetPrep {
  def main(args: Array[String]): Unit = {
    val inputs = args(0).split(",")
    val outputs =args(1).split(",")
    val idPath = args(2)
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val mapper = sc.broadcast(sc.textFile(idPath).flatMap(line => {
      try {
        val tmp = line.split(",")
        Some((tmp(0).stripMargin, Integer.parseInt(tmp(1))))
      } catch {
        case e: Exception => None
      }
    }).collect.toMap)

    inputs zip outputs foreach{
      case(input, output) =>
        if(PathUtil.isExisted(sc,output)){
          PathUtil.deleteExistedPath(sc, output)
        }
      val data = sc.textFile(input).flatMap{
        line =>
        val tmp = line.split(" ", 2)
        val label = mapper.value.getOrElse(tmp.head,2)
        if(label != 2){
          Some(s"$label ${tmp(1)}")
        } else {
          None
        }
      }.repartition(48).saveAsTextFile(output)
      }
    }
}
