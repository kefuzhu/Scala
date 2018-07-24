package com.talkingdata.dmp.etl.extract

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * only for binary target value
  * @author takun
  */
object Selector {
  def log(p:Double) = math.log(p) / math.log(2)
  def geni(p1:Double, p2:Double) = math.pow(p1, 2) + math.pow(p2, 2)
  def chi(x1:Double, x2:Double, e1:Double, e2:Double) = math.sqrt(math.pow(x1 - e1, 2) / e1) + math.sqrt(math.pow(x2 - e2, 2) / e2)
  val t = 1e-8
  def entropy(p1:Double, p2:Double) = {
    val v1 = if(p1 < t) t else p1
    val v2 = if(p2 < t) t else p2
    -v1 * log(v1) - v2 * log(v2)
  }

  def select(rdd:RDD[(String,Int)], fn:Int) = {
    val label_count = rdd.map(_._2).countByValue()
    val positive_count = label_count(1)
    val negative_count = label_count(0)
    val positive_prob = positive_count.toDouble / (positive_count + negative_count)
    val negative_prob = 1.0 - positive_prob
    rdd.map{
      case (key, label) =>
        val arr = Array.ofDim[Double](2)
        arr(label) += 1
        key -> arr
    }.reduceByKey({
      (a, b) =>
        a(0) += b(0)
        a(1) += b(1)
        a
    }, fn).map{
      case (key, values) =>
        val sum = values(0) + values(1)
        val neg_prob = values(0) / sum
        val pos_prob = values(1) / sum
        val neg_exp = negative_prob * sum
        val pos_exp = positive_prob * sum
        val _chi = chi(values(0), values(1), neg_exp, pos_exp)
        val _entropy = entropy(neg_prob, pos_prob)
        val _geni = geni(neg_prob, pos_prob)
        key -> Array(_chi, _entropy, _geni)
    }
  }
}

object AoiSelector {
  import Selector._

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Entropy")
    val sc = new SparkContext(conf)
    val input = args(0)
    val output = args(1)
    select(sc.textFile(input).map{
      line =>
        val spl = line.split("\t")
        spl(1) -> spl(2).toInt
    }, 4).map{
      case (key, values) => key + "\t" + values.mkString(",")
    }.saveAsTextFile(output)
  }
}
