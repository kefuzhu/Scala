package com.talkingdata.dmp.etl.extract

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.lit
import scala.collection.mutable

/**
  * Created by chris on 6/23/17.
  */
object AppRunExtract {
  def main(args:Array[String]):Unit = {
    val input = args(0)
    val output = args(1)
    val idPath = args(2)
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val id = sc.textFile(idPath).collect.toSeq
    val broadcastId = sc.broadcast(id)

    import sqlContext.implicits._
    val data = sqlContext.read.parquet(input).where($"tdid".isin(broadcastId.value.map(lit(_)): _*))

    data.registerTempTable("pd")

    sqlContext.sql("select tdid, appList.run as run from pd").flatMap {
      row =>
      val tdid = row.getAs[String]("tdid")
      val run = row.getAs[mutable.WrappedArray[Long]]("run")
      if (!run.isEmpty) {
        val fields = mutable.Map[Long, Int]()
        (0 until run.length).foreach { i =>
          fields += run(i) -> 1
        }
        Some(tdid -> fields)
      } else {
        None
      }
    }.reduceByKey({
      (m1,m2) => m2.foreach{
        case (k,v2) => m1.get(k) match {
          case Some(v) => m1 += k -> (v + v2)
          case None => m1 += k -> v2
        }
      }
        m1
    },24).map{
      case (tdid,fields) => tdid + "\t" + fields.map{case (k,v) => s"$k:$v"}.mkString(",")
    }.saveAsTextFile(output)
  }
}
