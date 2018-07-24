package com.talkingdata.dmp.etl.extract

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Row, SQLContext}
import java.nio.ByteBuffer
import org.apache.spark.{SparkConf, SparkContext}
import org.roaringbitmap.buffer.ImmutableRoaringBitmap

import scala.collection.mutable
/**
  * Created by chris on 6/29/17.
  */
object TimeFeatureExtractFreq {
  def main(args:Array[String]):Unit = {
    val input = args(0)
    val output = args(1)
    val idPath = args(2)
    val featureName = args(3)
    val feature = featureName match {
      case "pkg" => "pkgTimeSet"
      case "app" => "appTimeSet"
    }

    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val id = sc.textFile(idPath).collect.toSeq
    val broadcastId = sc.broadcast(id)

    import sqlContext.implicits._
    val data = sqlContext.read.parquet(input).where($"tdid".isin(broadcastId.value.map(lit(_)): _*))

    data.registerTempTable("pd")

    sqlContext.sql(s"select tdid, $feature as feature from pd").flatMap {
      row =>
        val tdid = row.getAs[String]("tdid")
        val feature = row.getAs[Seq[Row]]("feature")
        if (!feature.isEmpty) {
          val fields = mutable.Map[String, Int]()
          (0 until feature.length).foreach { i =>
            val bitmask = feature(i).getAs[Array[Byte]]("bitMask")
            val mapper = new ImmutableRoaringBitmap(ByteBuffer.wrap(bitmask)).toMutableRoaringBitmap
            val freq = mapper.toArray.size
            val count = fields.getOrElse(feature(i).getAs[String](featureName), freq)
            fields += feature(i).getAs[Any](featureName).toString -> (count + freq)
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
