package com.talkingdata.model.xgboost

import com.talkingdata.utils.{FileStream, LibSVM, PathUtil}
import ml.dmlc.xgboost4j.LabeledPoint
import ml.dmlc.xgboost4j.scala.DMatrix
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import ml.dmlc.xgboost4j.scala.XGBoost
import ml.dmlc.xgboost4j.scala.Booster
import util.control.Breaks._

/**
  * Created by chris on 7/21/17.
  */
object Prediction {

  def predByXg(sc:SparkContext,testSet: RDD[(String, LabeledPoint)],model:Booster)={
    val broadcastModel = sc.broadcast(model)
    testSet.map(_._2).mapPartitions{ iter =>
        broadcastModel.value.predict(new DMatrix(iter)).map(_.head).toIterator
    }.zip(testSet).map{case(pred, (tdid, feauture)) =>
        s"$tdid\t$pred"
    }
  }

  def loadModel(input:String) = XGBoost.loadModel(FileStream.getHDFSInputStream(input))

  def main(args: Array[String]): Unit = {
    val conf  =  new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val inputs = args(0).split(",")
    val outputs = args(1).split(",")
    val modelPaths = args(2).split(",")
    inputs zip outputs zip modelPaths foreach {
      case((input, output),modelPath) =>
        breakable{
          try{
            val model = loadModel(modelPath)
            val testSet = LibSVM.loadLibSVMFilePred(sc,input,-1,sc.defaultMinPartitions)
            val result = predByXg(sc, testSet,model)
            if(PathUtil.isExisted(sc,output)) {
              PathUtil.deleteExistedPath(sc, output)
            }
            result.saveAsTextFile(output)
          } catch {
            case e:Exception => break
          }
        }
    }

  }

}
