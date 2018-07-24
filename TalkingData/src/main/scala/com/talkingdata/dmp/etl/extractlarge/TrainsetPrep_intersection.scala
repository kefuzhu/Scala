package com.talkingdata.dmp.etl.extractlarge

import com.talkingdata.utils.PathUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by kefu on 4/11/2018.
  */

object TrainsetPrep_intersection {
  def main(args: Array[String]): Unit = {
    val inputs = args(0).split(",")
    val outputs =args(1).split(",")
    val idPath = args(2)
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val mapper = sc.broadcast(sc.textFile(idPath).flatMap(line => {
      try {
        val tmp = line.split("\t")
        Some((tmp(0).stripMargin, Integer.parseInt(tmp(1))))
      } catch {
        case e: Exception => None
      }
    }).collect.toMap)

    val labeled_tdid = sc.textFile(idPath).map{
      line => line.split("\t").head
    }.collect.toSet

    var tdid_intersection = Set[String]()

    var current_Set = Set[String]()
    var prev_Set = Set[String]()

    for(input <- inputs){

      // Find TDID in this feature dimension that have sex label
      val data = sc.textFile(input).map{
        line =>
          val tdid = line.split(" ").head
          tdid
      }.filter{
        tdid => labeled_tdid.contains(tdid)
      }.collect.toSet

      //If this is the first set
      if(prev_Set.isEmpty){
        prev_Set = data
      } else{ // This is not the first set
        // Get the current set of TDID
        current_Set = data
        // Assign the intersection of prev_Set and current_Set to tdid_intersection
        tdid_intersection = prev_Set & current_Set
        // Change the value of prev_Set to current_Set, prepare for the next interation
        prev_Set = current_Set
      }
    }

    current_Set = null
    prev_Set = null

    println("The number of TDID in intersection is " + tdid_intersection.size.toString)

    inputs zip outputs foreach{
      case(input, output) =>
        if(PathUtil.isExisted(sc,output)){
          PathUtil.deleteExistedPath(sc, output)
        }
        val data = sc.textFile(input).filter{
          line => tdid_intersection.contains(line.split(" ").head)
        }.map{
          line =>
            val tmp = line.split(" ", 2)
            val label = mapper.value.getOrElse(tmp.head,2)
            if(label != 2){
              Some(s"${tmp.head} $label ${tmp(1)}")
            } else {
              None
            }
        }.repartition(48).saveAsTextFile(output)
    }
  }
}

