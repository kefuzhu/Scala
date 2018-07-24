package com.talkingdata.dmp.etl.extractlarge

import com.talkingdata.utils.PathUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by kefu on 01/25/18.
  */
object TrainsetPrep_balance {
  def main(args: Array[String]): Unit = {
    val inputs = args(0).split(",")
    val outputs =args(1).split(",")
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

//    val ratio = 4.0/6.0
    val ratio = 5.0/5.0

    inputs zip outputs foreach{
      case(input, output) =>
        if(PathUtil.isExisted(sc,output)){
          PathUtil.deleteExistedPath(sc, output)
        }

        val data = sc.textFile(input).map{
          line =>
            val col = line.split(" ")
            val label = col(0)
            val feature = col(1)
            (label,feature)
        }
        //Count the number of female records and male records in this dataset
        val female_number = data.filter{
          case(label,feature) => label == "0"
        }.count
        val male_number = data.filter{
          case(label,feature) => label == "1"
        }.count
        //Calculate the maximum number of records we can have in order to have 5:5 for sex ratio
        val num = Array(female_number,male_number).min.toInt
        val ratioNum = ((num.toDouble)*ratio).toInt
        //Extract the female data and remain the number of records we want
        val female_data = sc.parallelize(data.filter{
          case(label,feature) => label == "0"
        }.take(num))
        //Extract the male data and remain the number of records we want
        val male_data = sc.parallelize(data.filter{
          case(label,feature) => label == "1"
        }.take(ratioNum))
        //Concatenate two RDD together and output
        val balanced_data = female_data.union(male_data).map{
          case(label,feature) =>
            label + " " + feature
        }.repartition(48).saveAsTextFile(output)
    }
  }
}
