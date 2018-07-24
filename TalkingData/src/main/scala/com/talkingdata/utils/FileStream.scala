package com.talkingdata.utils

/**
  * Created by chris on 7/20/17.
  */
import java.io._
import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.hadoop.util.Progressable

object FileStream {

  def getHDFSInputStream( hdfsPath: String ): InputStream = {
    try {
      val conf = new Configuration
      val fs = FileSystem.get(URI.create(hdfsPath), conf)
      val fsr = fs.open(new Path(hdfsPath))
      fsr
    } catch {
      case e: IOException =>
        e.printStackTrace()
        null
    }
  }

  def getHDFSOutputStream( hdfsPath: String): OutputStream = {

    try {
      val conf = new Configuration
      val fs = FileSystem.get(URI.create(hdfsPath), conf)
      val out = fs.create(new Path(hdfsPath), new Progressable() {
        override def progress(): Unit = {
        }
      })
      out
    } catch {
      case e: Exception =>
        e.printStackTrace()
        null
    }
  }

}