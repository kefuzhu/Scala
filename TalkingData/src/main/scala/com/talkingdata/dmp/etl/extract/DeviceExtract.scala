package com.talkingdata.dmp.etl.extract


import com.talkingdata.utils.HandyFunc
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.Row
/**
  * Created by chris on 7/14/17.
  */
object DeviceExtract {
  def main(args: Array[String]): Unit = {
    val input = args(0)
    val output = args(1)
    val end = args(2)
    val days = args(3)
    val part = args(4)
    val day = HandyFunc.range(end, days.toInt)
    val conf = new SparkConf().setAppName("Device")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //initialize DF
    val schema = StructType(
      StructField("tdid", StringType, true) ::
        StructField("model", StringType, true) ::
        StructField("standardModel", StringType, true)::
        StructField("brand",StringType,true)::
        StructField("pixel",StringType,true)::Nil)
    var initialDF = sqlContext.createDataFrame(sc.emptyRDD[Row], schema)
    for(each <- day){
      val path = s"$input/$each/*/*"
      initialDF.unionAll(sqlContext.read.parquet(path).select("tdid","device.model","device.standardModel","device.brand","device.pixel").toDF("tdid","model","standardModel","brand","pixel").distinct())
    }
    initialDF.distinct().repartition(part.toInt).write.parquet(output)
  }
}
