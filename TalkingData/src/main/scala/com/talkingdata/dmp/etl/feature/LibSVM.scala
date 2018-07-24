package com.talkingdata.dmp.etl.feature

import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer
/**
  * Created by chris on 6/30/17.
  */
object LibSVM {
  case class featureTuple(
                         feature:String,
                         weight:Int
                         )

  def main(args:Array[String]): Unit = {
    val input = args(0)
    val output = args(1)
    val weight = args(3) match {
      case "weight" => true
      case _ => false
    }
    val start = Integer.parseInt(args(3))

    val conf = new SparkConf().setAppName("Entropy")
    val sc = new SparkContext(conf)
    val data = sc.textFile(input).flatMap{f =>
      try{
        val tmp = f.split("\t")
        val label = tmp(0)
        val features = tmp(1).split(",")
        val vector = new ArrayBuffer[featureTuple]()
        if(weight) {
          (0 until features.length).foreach { i =>
            val feature = features(i).split(":")
            vector += featureTuple(feature(0), Integer.parseInt(feature(1)))
          }
        } else {
            (0 until features.length).foreach{ i =>
              val feature = features(i).split(":")
              vector += featureTuple(feature(0), 1)
            }
        }
        Some(label -> vector)
      } catch {
        case e:Exception => None
      }
    }
    val featureMap = data.flatMap{ f =>
      f._2.map(f => f.feature)
    }
      .collect()
      .distinct zip (Stream from start)

    val broadcastFeatureMap = sc.broadcast(featureMap.toMap)
    data.mapPartitions{
      f =>
      val mapper = broadcastFeatureMap.value
      val result = new ArrayBuffer[String]()
      while(f.hasNext){
        val line = new ArrayBuffer[String]()
        val (label, feature) = f.next()
        line += label
        (0 until feature.length).foreach{
          i =>
          line += s"${mapper.getOrElse(feature(i).feature,0)}:${feature(i).weight}"
        }
        result += line.mkString(" ")
      }
      result.toIterator
    }

  }


}
