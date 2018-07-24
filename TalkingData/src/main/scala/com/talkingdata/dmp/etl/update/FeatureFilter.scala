package com.talkingdata.dmp.etl.update

import com.talkingdata.utils.PathUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.functions.{col, udf}

/**
  * Created by chris on 7/27/17.
  */
object FeatureFilter {
  def main(args:Array[String]):Unit = {
    val input = args(0)
    val outputs = args(1).split(",")
    val idPath = args(2)
    val cols = args(3).split(",")/*Array("pkgTimeSet","pkgTimeSet","idBox")*/
    val fields = args(4).split(",") /*Array("appKey","pkg","model")*/
    val tmpOutput = args(5)
    val sparkConf = new SparkConf()
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
    if(PathUtil.isExisted(sc,tmpOutput)){
      PathUtil.deleteExistedPath(sc,tmpOutput)
    }
    val data = sqlContext.read.parquet(input).select("tdid", cols.distinct:_*).filter(sqlfunc(col("tdid"))).write.parquet(tmpOutput)
    val dataNow = sqlContext.read.parquet(tmpOutput)
    outputs zip cols zip fields foreach{
      case((output, col), field) =>
        if(PathUtil.isExisted(sc,output)){
          PathUtil.deleteExistedPath(sc, output)
        }
      dataNow.flatMap{ row =>
        val mappers = broadcastMapper.value
        val tdid = row.getAs[String]("tdid")
        val label = mappers.getOrElse(tdid, 2)
        val ev = row.getAs[Seq[Row]](col).flatMap{r =>
          try{
            val a = r.getAs[String](field).stripSuffix("\"").stripPrefix("\"")
            Some(s"$tdid\t$a\t$label")
          } catch {
            case e:Exception => None
          }
        }.distinct
        ev
      }.repartition(128).saveAsTextFile(output)
    }
    if(PathUtil.isExisted(sc,tmpOutput)){
      PathUtil.deleteExistedPath(sc,tmpOutput)
    }
  }
}
