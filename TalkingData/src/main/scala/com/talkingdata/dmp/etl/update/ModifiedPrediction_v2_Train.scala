package com.talkingdata.dmp.etl.update

import com.talkingdata.utils.{FileStream, PathUtil}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.UnionRDD
import org.apache.spark.{SparkConf, SparkContext}
import ml.dmlc.xgboost4j.scala.spark.{XGBoost => sparkXGBoost}
import scala.collection.mutable.ArrayBuffer

/**
  * Created by Kefu on 12/08/17.
  */
object ModifiedPrediction_v2_Train {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val input = args(0).split(",")
    val extractFeature = args(1)
    val trainSet = args(2)
    val partition = args(3).toInt
    val numRound = args(4).toInt
    val workers = args(5).toInt
    val modelOutput = args(6)

    val inputNow = input.filter(PathUtil.isExisted(sc, _))

    //  Label Map
    val tdid_label = sc.textFile("/datalab/user/kefu/gender_groundTruth").map{
      line =>
        val col = line.split('\t')
        val sex = col(0)
        val tdid = col(2)
        tdid -> sex
    }.collectAsMap()

    //  Get Max, Min & numMissingFeature
    val data = new UnionRDD(sc,input.map{path =>
      sc.textFile(path).flatMap{line =>
        try{
          Some(line.split("\t")(0) -> ArrayBuffer(line.split("\t")(1).toFloat))
        } catch {
          case e:Exception => None
        }
      }
    }).reduceByKey({
      (m1,m2) => m1 ++ m2
    },partition).flatMap{case (tdid, feature)=>
      try{
        val missingFeature = input.length - feature.length
        val max = feature.max
        val min = feature.min
        Some((tdid,max,min,missingFeature))
      } catch {
        case e:Exception => None
      }
    }

    data.map{
      case((tdid,max,min,missingFeature)) =>
        tdid + "\t" + max + "\t" + min + "\t" + missingFeature
    }.saveAsTextFile(extractFeature)

    //  Map labeled data
    val labeled_data = data.map{
      case(tdid,max,min,missingFeature) =>
        val label = tdid_label.getOrElse(tdid,"Unknown")
        (label,tdid,max,min,missingFeature)
    }.filter{
      case(label,tdid,max,min,missingFeature) =>
        label != "Unknown"
    }

    labeled_data.map{
      case(label,tdid,max,min,missingFeature) =>
        label + "\t" + tdid + "\t" + max + "\t" + min + "\t" + missingFeature
    }.saveAsTextFile(trainSet)


    //  Train XGBoost model on top of max/min score & numMissingFeature
    val trainRDD = labeled_data.map{
      case(label,tdid,max,min,missingFeature) =>
        LabeledPoint(label.toDouble, Vectors.dense(max.toDouble,min.toDouble))
    }


    val paramMap = List("objective" -> "binary:logistic",
      "max_depth" -> 2).toMap

    //    val paramMap = List("objective" -> "binary:logistic",
    //                        "eta" -> 0.3,
    //                        "gamma" -> 0.0,
    //                        "max_depth" -> 2,
    //                        "colSampleByTree" -> 0.8,
    //                        "subSample" -> 0.5,
    //                        "minChildWeight" -> 1.0).toMap

    val model = sparkXGBoost.train(trainRDD, paramMap, numRound, nWorkers = workers, useExternalMemory = true)
    model.booster.saveModel(FileStream.getHDFSOutputStream(modelOutput))
  }
}
