package com.talkingdata.dmp.etl.extract

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by chris on 7/11/17.
  */
object PkgExtract {
  def main(args: Array[String]): Unit = {
    val input = args(0)
    val output = args(1)
    val id = args(2)
    val choice = args(3) match {
      case "app" => "app.appName"
      case "pkg" => "app.pkgName"
    }
    val conf = new SparkConf().setAppName("package")
    val sc = new SparkContext()
    val sqlContext = new SQLContext(sc)
    val mapper = sc.textFile(id).flatMap(line => {
      try {
        val tmp = line.split(",")
        Some((tmp(0).stripMargin, Integer.parseInt(tmp(1))))
      } catch {
        case e: Exception => None
      }
    }).collect.toMap
    val broadcastMapper = sc.broadcast[Map[String,Int]](mapper)
    val data = sqlContext.read.parquet(input)
    data.registerTempTable("pd")
    sqlContext.sql(s"select tdid, $choice as choice from pd").flatMap{line =>
      try{
        val tdid = line.getAs[String]("tdid")
        val name = line.getAs[String]("choice")
        val label = broadcastMapper.value.getOrElse(tdid, 2)
        if(label != 2) {
          Some(s"$tdid\t$name\t$label")
        } else {
          None
        }
      } catch {
        case e:Exception => None
      }
    }.distinct.repartition(24).saveAsTextFile(output)
  }
}
