package com.talkingdata.dmp.etl.update
import com.talkingdata.utils.PathUtil
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by chris on 7/27/17.
  */
object SampleUpdate {
  def main(args: Array[String]): Unit = {
    val inputOrigin = args(0)
    val inputUpdate = args(1)
    val output = args(2)
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    if(PathUtil.isExisted(sc,inputUpdate)){
      sc.textFile(inputOrigin).union(sc.textFile(inputUpdate)).distinct(2).saveAsTextFile(output)
    } else {
      sc.textFile(inputOrigin).saveAsTextFile(output)
    }
  }
}
