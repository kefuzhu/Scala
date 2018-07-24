package com.talkingdata.model.xgboost

import com.talkingdata.utils.{FileStream, LibSVM}
import ml.dmlc.xgboost4j.scala.spark.XGBoost
import org.apache.spark.mllib.regression.LabeledPoint
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.control.Breaks._

/**
  * Created by kefu
  */
object Train_test{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    val modelOutput = "/datalab/user/kefu/test/xgboost_testModel"
    val numRound = 1500
    val workers = 4
    val paramMap = List("objective" -> "binary:logistic", "seed" -> 12345).toMap

    var guess_random = scala.util.Random
    val size = 100000
    val feature_size = 100000*10
    val label_higher = 2
    val value_higher = 100
    val feature_higher = feature_size

    var data = ArrayBuffer[LabeledPoint]()
    for (i <- 0 until feature_size){
      val feature_array = ArrayBuffer[Int]()
      val value_array = ArrayBuffer[Double]()
      for(j <- 0 until 7){
        var feature = guess_random.nextInt(feature_higher)
        var value = guess_random.nextInt(value_higher).toDouble

        feature_array += feature
        value_array += value
      }
      val label = guess_random.nextInt(label_higher).toDouble
      data += LabeledPoint(label, Vectors.sparse(feature_size,feature_array.toArray,value_array.toArray))
    }

    val trainRDD = sc.parallelize(data)
    val model = XGBoost.train(trainRDD, paramMap, numRound, nWorkers = workers, useExternalMemory = true)
    model.booster.saveModel(FileStream.getHDFSOutputStream(modelOutput))
  }
}
