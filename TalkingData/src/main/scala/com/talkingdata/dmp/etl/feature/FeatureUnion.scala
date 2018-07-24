package com.talkingdata.dmp.etl.feature

import org.apache.spark.rdd.UnionRDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by chris on 7/19/17.
  */
object FeatureUnion {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Union")
    val sc = new SparkContext(conf)
    val input = args(0).split(",")
    val output = args(1)
    val partition = args(2).toInt
    val data = new UnionRDD(sc,input.map{path =>
      sc.textFile(path).flatMap{line =>
        try{
          Some(line.split("\t")(0) -> ArrayBuffer(line.split("\t")(1)))
        } catch {
          case e:Exception => None
        }
      }
    }).reduceByKey({
      (m1,m2) => m1 ++ m2
    },partition)
    .map{case (tdid, features) =>
        s"$tdid\t${features.mkString(" ")}"
    }.saveAsTextFile(output)
  }
}
