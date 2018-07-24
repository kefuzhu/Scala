package com.talkingdata.dmp.etl.update

import org.apache.spark.{SparkConf, SparkContext}
import com.talkingdata.utils.{FileStream, PathUtil}
import org.apache.spark.rdd.RDD
import ml.dmlc.xgboost4j.scala.{Booster, DMatrix, XGBoost}
import ml.dmlc.xgboost4j.{LabeledPoint => MLLabeledPoint}

/**
  * Created by Kefu on 12/08/17.
  */
object ModifiedPrediction_v2_Predict{

  def predByXg(sc:SparkContext,testSet: RDD[(String, MLLabeledPoint)],model:Booster)={
    val broadcastModel = sc.broadcast(model)
    testSet.map(_._2).mapPartitions{ iter =>
      broadcastModel.value.predict(new DMatrix(iter)).map(_.head).toIterator
    }.zip(testSet).map{case(pred, (tdid, feauture)) =>
      s"$tdid\t$pred"
    }
  }

  def loadModel(input:String) = XGBoost.loadModel(FileStream.getHDFSInputStream(input))

  def main(args: Array[String]): Unit = {
    val input = args(0)
    val modelPath = args(1)
    val outputMale = args(2)
    val outputFemale = args(3)
    val conf = new SparkConf()
    val sc = new SparkContext(conf)


    val maleTagID = "304"
    val femaleTagID = "305"

    if(PathUtil.isExisted(sc,outputMale)){
      PathUtil.deleteExistedPath(sc, outputMale)
    }

    if(PathUtil.isExisted(sc,outputFemale)){
      PathUtil.deleteExistedPath(sc, outputFemale)
    }

    val model = loadModel(modelPath)

    val testRDD = sc.textFile(input).map{
      line =>
        val col = line.split("\t")
        val label = col(0).toFloat
        val tdid = col(1)
        val max = col(2).toFloat
        val min = col(3).toFloat
        val missingFeature = col(4).toFloat
        (tdid,MLLabeledPoint.fromDenseVector(label, Array(max, min, missingFeature)))
    }

//    val result = predByXg(sc, testRDD, model).map{line =>
//      try{
//        Some((line.split("\t")(0),line.split("\t")(1).toFloat))
//      } catch {
//        case e:Exception => None
//      }
//    }

    val result = predByXg(sc, testRDD, model).map{
      line =>
        val col = line.split("\t")
        val tdid = col(0)
        val score = col(1).toFloat
        (tdid,score)
    }

//  Male Prediction
    result.filter(f => f._2 >= 0.65).sortBy(-_._2).map(r=>s"${r._1}\t$maleTagID\t${100*r._2}").coalesce(512).saveAsTextFile(outputMale)
//  Female Prediction
    result.filter(f => f._2 <= 0.4).sortBy(-_._2).map(r=>s"${r._1}\t$femaleTagID\t${100*(1 - r._2)}").coalesce(512).saveAsTextFile(outputMale)

  }
}
