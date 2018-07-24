package com.talkingdata.dmp.etl.extract

import com.talkingdata.dmp.util.DateUtil
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.lit

/**
  * Created by chris on 6/20/17.
  */



object GeoExtract {
  def main(args:Array[String]):Unit = {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val id =  sc.textFile(args(0)).collect.toSeq
    val broadcastId = sc.broadcast(id)
    val start = "2016-12-01"
    val end = "2016-12-31"
    val prefix = "/data/datacenter/solar-system/Location"
    val output = args(1)
    import sqlContext.implicits._
    var counter = 0
    DateUtil.range(start,end,"weekday").foreach{
      f =>
        counter += 1
        val path = prefix + "/" + f
        val outputPath = output + "/" + counter
        val data = sqlContext.read.parquet(path).where($"tdid".isin(broadcastId.value.map(lit(_)):_*)).write.json(outputPath)
        }
    sc.stop()
    }
}
