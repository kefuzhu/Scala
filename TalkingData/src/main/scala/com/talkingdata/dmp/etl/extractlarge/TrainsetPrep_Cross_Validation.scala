package com.talkingdata.dmp.etl.extractlarge

import com.talkingdata.utils.PathUtil
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by kefu on 5/2/18.
  */
object TrainsetPrep_Cross_Validation {
  def main(args: Array[String]): Unit = {
    val inputs = args(0).split(",")
    val train_outputs = args(1).split(",")
    val test_outputs = args(2).split(",")
    val train_ratio = args(3).toDouble
    val idPath = args(4)
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    // Combine train_output and test_output
    var outputs = ArrayBuffer[Array[String]]()

    train_outputs zip test_outputs foreach{
      case(train_output,test_output) => outputs += Array(train_output,test_output)
    }

    // Ground Truth Map
    val mapper = sc.broadcast(sc.textFile(idPath).flatMap(line => {
      try {
        val tmp = line.split("\t")
        Some((tmp(0).stripMargin, Integer.parseInt(tmp(1))))
      } catch {
        case e: Exception => None
      }
    }).collect.toMap)

    inputs zip outputs foreach{
      case(input, output) =>

        val train_out = output(0)
        val test_out= output(1)

        //5-fold Cross-Validation (Prepare Train/Test Data)
        for(i<-Array(1,2,3,4,5)){

          var cv_train_out = ""
          var cv_test_out = ""

          val data = sc.textFile(input).flatMap{
            line =>
              val tmp = line.split(" ", 2)
              val label = mapper.value.getOrElse(tmp.head,2)
              if(label != 2){
                Some(s"$label ${tmp.head}\t${tmp(1)}")
              } else {
                None
              }
          }.randomSplit(Array(train_ratio, 1-train_ratio), seed = i)

          cv_train_out = train_out + "/" + i.toString()
          cv_test_out = test_out + "/" + i.toString()

          if(PathUtil.isExisted(sc,cv_train_out)){
              PathUtil.deleteExistedPath(sc, cv_train_out)
          }
          if(PathUtil.isExisted(sc,cv_test_out)){
            PathUtil.deleteExistedPath(sc, cv_test_out)
          }

          // Save the larger data split as training data, the other part as testing data
          if(data(0).count > data(1).count){
            data(0).repartition(48).saveAsTextFile(cv_train_out)
            data(1).repartition(48).saveAsTextFile(cv_test_out)
          } else{
            data(0).repartition(48).saveAsTextFile(cv_test_out)
            data(1).repartition(48).saveAsTextFile(cv_train_out)
          }
        }
    }
  }
}
