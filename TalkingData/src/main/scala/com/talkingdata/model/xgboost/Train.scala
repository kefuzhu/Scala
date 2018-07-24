package com.talkingdata.model.xgboost

import org.apache.spark.{SparkConf, SparkContext}
import ml.dmlc.xgboost4j.scala.spark.XGBoost
import com.talkingdata.utils.{FileStream, LibSVM, PathUtil}
import util.control.Breaks._
import org.apache.spark.mllib.clustering.LDAModel

/**
  * Created by chris on 7/20/17.
  *
  * Modified by kefu on 3/22/2018
  *   Add seed control
  */
object Train{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val inputTrain = args(0).split(",")
    val modelOutput = args(1).split(",")
    val numRound = args(2).toInt
    val workers = args(3).toInt
    val paramMap = List("objective" -> "binary:logistic", "seed" -> 12345).toMap
    val paths = inputTrain zip modelOutput foreach { case (input, output) =>
      breakable {
        try {
          val trainRDD = LibSVM.loadLibSVMFile(sc, input)
          val model = XGBoost.train(trainRDD, paramMap, numRound, nWorkers = workers, useExternalMemory = false)
          model.booster.saveModel(FileStream.getHDFSOutputStream(output))
        } catch {
          case e: Exception => break
        }
      }
    }
  }
}
