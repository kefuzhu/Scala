package com.talkingdata.model.xgboost

import com.talkingdata.utils.{FileStream, LibSVM_tmp, PathUtil}
import ml.dmlc.xgboost4j.LabeledPoint
import ml.dmlc.xgboost4j.scala.{Booster, DMatrix, XGBoost}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.control.Breaks._

/**
  * Created by kefu on 5/3/18.
  */
object Prediction_Cross_Validation {

  def predByXg(sc:SparkContext,testSet: RDD[(String, String, LabeledPoint)],model:Booster)={
    val broadcastModel = sc.broadcast(model)
    testSet.map(_._3).mapPartitions{ iter =>
        broadcastModel.value.predict(new DMatrix(iter)).map(_.head).toIterator
    }.zip(testSet).map{case(pred, (label, tdid, feauture)) =>
        s"$tdid\t$label\t$pred"
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
            val testSet = LibSVM_tmp.loadLibSVMFilePred(sc,input,-1,sc.defaultMinPartitions)
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
