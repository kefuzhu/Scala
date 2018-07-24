package com.talkingdata.dmp.etl.extractlarge

import com.talkingdata.utils.PathUtil
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by chris on 7/24/17.
  */
object AppPkg {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val input = args(0)
    val output = args(1)
    val entrySetPath = args(2)
    val choice = args(3) match {
      case "app" => "pkgTimeSet"
      case "pkg" => "pkgTimeSet"
    }

    val field = args(3) match {
      case "app" => "appKey"
      case "pkg" => "pkg"
    }

    val dim = args(4).toInt


    val sqlContext = new SQLContext(sc)
    val mapper = sc.broadcast(sc.textFile(entrySetPath).flatMap(line => {
      try {
        val tmp = line.split(",")
        Some((tmp(0).stripMargin, Integer.parseInt(tmp(1))))
      } catch {
        case e: Exception => None
      }
    }).collect.slice(0,dim).toMap)
    if(PathUtil.isExisted(sc,output)){
      PathUtil.deleteExistedPath(sc, output)
    }

    val data = sqlContext.read.parquet(input).select("tdid", choice).flatMap{
      f=>
        val map = mapper.value
        val tdid = f.getAs[String]("tdid")
        val envs = f.getAs[Seq[Row]](choice).flatMap{env =>
          val key =  env.getAs[String](field)
          map.get(key)
        }.map{f =>
          s"$f:1"
        }
        if(envs.nonEmpty){
          Some(s"$tdid ${envs.distinct.mkString(" ")}")
        } else {
          None
        }
    }.repartition(1000).saveAsTextFile(output)
  }
}
