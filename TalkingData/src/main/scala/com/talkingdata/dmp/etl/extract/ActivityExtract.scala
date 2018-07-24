package com.talkingdata.dmp.etl.extract
import com.tendcloud.tenddata.entity.EventPackage
import org.apache.hadoop.io.{BytesWritable, Text}
import org.apache.spark.{SparkConf, SparkContext}
import org.msgpack.ScalaMessagePack
import org.msgpack.unpacker.BufferUnpacker
import scala.collection.mutable.ArrayBuffer
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

/**
  * Created by chris on 7/7/17.
  */
object ActivityExtract {
  def apply(headerBytes: Array[Byte], bodyBytes: Array[Byte]) = {
    try {
      val receiveTime = compact(parse(new String(headerBytes, "UTF-8")) \\ "ts").toLong
      val unPacker: BufferUnpacker = ScalaMessagePack.messagePack.createBufferUnpacker(bodyBytes)
      val data = unPacker.read[EventPackage](classOf[EventPackage])
      val id = data.getmDeviceProfile().getmOsVersion()
      val tdid = data.getmDeviceId.toLowerCase.replaceAll("[^0-9a-z]", "")
      val app = data.getmAppProfile().getmAppPackageName()
      val msgs = data.getmTMessages
      val fields = ArrayBuffer[(String, Long, Int, String)]()
      (0 until msgs.size ).foreach{
        i =>
          val msg = msgs.get(i)
          val session = msg.getSession
          if( session != null ) {
            val activities = session.getActivities
            (0 until activities.size).foreach {
              j =>
                val activity = activities.get(j)
                val activityStart = activity.start
                val activityID = activity.name
                val activityRefer = activity.refer
                val activityDuration = activity.duration
                if( activityID != null) fields += ((activityID,activityStart, activityDuration, activityRefer))

            }
          }
      }
      if(!fields.isEmpty && !app.contains("systemui")){
        val json = ("tdid" -> tdid) ~ ("appKey" -> app.stripPrefix("\"").stripSuffix("\"")) ~ ("time" -> receiveTime) ~ ("activities" -> fields.toList.map{
          case(name, start, duration, refer) =>
            (("name" -> name)~("start"->start)~("duration"->duration)~("refer"->refer))
        })
        Some(compact(render(json)))
      } else {
        None
      }

    } catch {
      case e: Exception => None
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("tdcv3")
    val sc = new SparkContext(conf)
    val r = sc.sequenceFile[Text, BytesWritable](args(0)).flatMap {
      case (key, value) =>
        val len = value.getLength
        val index = value.getBytes.indexOf('\0'.toByte)
        val headerBytes = value.getBytes.slice(0, index)
        val bodyBytes = value.getBytes.slice(index + 1, len)
        apply(headerBytes, bodyBytes)
    }.repartition(48).saveAsTextFile(args(1))
  }
}
