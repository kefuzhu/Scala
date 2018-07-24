package com.talkingdata.model.xgboost

import com.talkingdata.utils.{FileStream, LibSVM_tmp}
import ml.dmlc.xgboost4j.scala.spark.XGBoost
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.control.Breaks._

/**
  * Created by kefu on 5/2/18.
  *
  */
object Train_Cross_Validation{

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
          val trainRDD = LibSVM_tmp.loadLibSVMFile(sc, input)
          val model = XGBoost.train(trainRDD, paramMap, numRound, nWorkers = workers, useExternalMemory = false)
          model.booster.saveModel(FileStream.getHDFSOutputStream(output))
        } catch {
          case e: Exception => break
        }
      }
    }
  }
}
