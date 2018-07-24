package com.talkingdata.dmp.etl.feature

import math.log
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by chris on 6/29/17.
  */
object Entropy {
  def apply(line:String) = {
    try {
      val tmp = line.split("\t")
      val label = Integer.parseInt(tmp(0))
      val variables = tmp(1).split(",")
      val variable2Label = new ArrayBuffer[(String, (Int, Int))]
      (0 until variables.length).foreach { i =>
        variable2Label += (variables(i) -> (label, 1))
      }
      Some(variable2Label)
    } catch {
      case e:Exception => None
    }
  }

  def calculateEntropy(numbers:Array[Double]) = {
    try {
      var totalProbability = 0d
      numbers.foreach { number =>
        totalProbability += -number*log(number + 0.0000001d)
      }
      totalProbability
    } catch {
      case e:Exception => Double.PositiveInfinity
    }

  }

  def Entropy(data:RDD[String], pos:Long,neg:Long)= {
    data.flatMap{
      line =>
        try{
          val spl = line.split("\t")
          Some(spl(1) -> (spl(2).toInt,1))
        } catch {
          case e:Exception => None
        }
    }.reduceByKey({
      (m1, m2) => (m1._1 + m2._1, m1._2 + m2._2)
    },4)
      .flatMap{ f =>
        try{
          val (variable, (positive, len)) = f
          val posRate1 = positive.toDouble / len
          val negRate1 = 1 - posRate1
          val entropy1 = len.toDouble / (pos + neg) * calculateEntropy(Array(posRate1,negRate1))
          val posRate2 = (pos - positive).toDouble / (pos + neg - len)
          val negRate2 = 1 - posRate2
          val entropy2 = (pos + neg - len).toDouble / (pos + neg) * calculateEntropy(Array(posRate2, negRate2))
          Some((variable,entropy1+entropy2,f._2._2))
        } catch {
          case e:Exception => None
        }
      }.sortBy(f=> f._2)

  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Entropy")
    val sc = new SparkContext(conf)
    val data = sc.textFile(args(0))
    val sampleMap = data.map {f =>
      f.split("\t")(2)
    }.countByValue()
    val pos = sampleMap.getOrElse("1", 3l)
    val neg = sampleMap.getOrElse("0", 3l)
    val d = Entropy(data,pos,neg).map{f=>
      s"${f._1}\t${f._2}\t${f._3}"
    }.saveAsTextFile(args(1))
  }
}
