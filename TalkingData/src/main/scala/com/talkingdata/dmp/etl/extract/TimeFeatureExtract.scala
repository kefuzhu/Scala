package com.talkingdata.dmp.etl.extract

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.col


/**
  * Created by chris on 6/27/17.
  */
object TimeFeatureExtract {
  def main(args:Array[String]):Unit = {
    val input = args(0)
    val output = args(1)
    val idPath = args(2)
    val featureName = args(3)
    val cols = featureName match {
      case "app" => "appTimeSet"
      case "pkg" => "pkgTimeSet"
    }
    val field = featureName match {
      case "app" => "app"
      case "pkg" => "pkg"
    }

    val sparkConf = new SparkConf().setAppName(s"$cols")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val mapper = sc.textFile(idPath).flatMap(line => {
      try {
        val tmp = line.split(",")
        Some((tmp(0).stripMargin, Integer.parseInt(tmp(1))))
      } catch {
        case e: Exception => None
      }
    }).collect.toMap
    val broadcastMapper = sc.broadcast[Map[String,Int]](mapper)
    val func: (String => Boolean) = (arg: String) => broadcastMapper.value.contains(arg)
    val sqlfunc = udf(func)
    val data = sqlContext.read.parquet(input).select("tdid", s"$cols").filter(sqlfunc(col("tdid")))
      .flatMap{
      row =>
        val mappers = broadcastMapper.value
        val tdid = row.getAs[String]("tdid")
        val label = mappers.getOrElse(tdid, 2)
        val ev = row.getAs[Seq[Row]](s"$cols").map{row =>
          val a = field match {
            case "app"  => row.getAs[Long](field).toString
            case "pkg" => row.getAs[String](field)
          }
          s"$tdid\t$a\t$label"
        }.distinct
        ev
    }.repartition(48).saveAsTextFile(output)
  }
}
