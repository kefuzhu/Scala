package com.talkingdata.model.eval
import org.apache.spark.rdd.RDD

/**
  * Created by chris on 7/4/17.
  */
object Accuracy {

  def compute(p:Float,y:Float) = if( y == p ) 1 else 0

  def of(rdd:RDD[(Float,Float)]) = {
    val (sum,size) = rdd.map{
      case (p,y) =>
        val v = compute(p,y)
        (v,1)
    }.treeReduce{
      case ((v1,s1),(v2,s2)) =>
        (v1 + v2 , s1 + s2)
    }
    sum.toDouble / size.toDouble
  }

}
