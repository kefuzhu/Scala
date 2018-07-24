package com.talkingdata.utils

/**
  * Created by chris on 7/27/17.
  */
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem

object PathUtil {

  /**
    * 判断目录是否存在
    */
  def isExisted(sc: SparkContext, path: String) = {
    val hdfs = FileSystem.get(sc.hadoopConfiguration)
    hdfs.exists(new Path(path)) match {
      case true =>
        println("this output path is existed:" + hdfs.makeQualified(new Path(path))); true
      case _ => false
    }
  }

  /**
    * 删除已存在的目录文件
    */
  def deleteExistedPath(sc: SparkContext, outputPath: String) = {
    val hdfs = FileSystem.get(sc.hadoopConfiguration)
    try {
      hdfs.delete(new Path(outputPath), true)
    } catch { case e: Throwable => throw (e) }
  }

  /**
    * 判断目录是否存在
    * param: SparkContext Path
    */
  def isExisted(sc: SparkContext, path: Path){
    val hdfs = FileSystem.get(sc.hadoopConfiguration)
    hdfs.exists(path) match{
      case true =>
        println("this output path is existed:"+ hdfs.makeQualified(path));true
      case _ => false
    }
  }
  /**
    * 删除已存在的目录文件
    */
  def deleteExistedPath(sc: SparkContext, outputPath: Path){
    val hdfs = FileSystem.get(sc.hadoopConfiguration)
    try{
      hdfs.delete(outputPath, true)
    }catch{
      case e: Throwable => throw(e)
    }
  }

}