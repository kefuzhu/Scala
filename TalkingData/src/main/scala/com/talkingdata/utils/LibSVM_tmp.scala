package com.talkingdata.utils

import ml.dmlc.xgboost4j.{LabeledPoint => MLLabeledPoint}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
  * Created by kefu on 5/2/18.
  */
object LibSVM_tmp {
  def loadLibSVMFile(sc: SparkContext, path: String): RDD[LabeledPoint] =
    loadLibSVMFile(sc, path, -1)

  def loadLibSVMFile(
                      sc: SparkContext,
                      path: String,
                      numFeatures: Int): RDD[LabeledPoint] =
    loadLibSVMFile(sc, path, numFeatures, sc.defaultMinPartitions)

  def loadLibSVMFile(
                      sc: SparkContext,
                      path: String,
                      numFeatures: Int,
                      minPartitions: Int): RDD[LabeledPoint] = {
    val parsed = sc.textFile(path, minPartitions)
      .map(_.trim)
      .filter(line => !(line.isEmpty || line.startsWith("#")))
      .map { line =>
        val items = line.split('\t')
        val label = items.head.split(" ").head.toDouble
        val (indices, values) = items.tail.filter(_.nonEmpty).mkString("").split(" ").map { item =>
          val indexAndValue = item.split(':')
          val index = indexAndValue(0).toInt - 1// Convert 1-based indices to 0-based.
        val value = indexAndValue(1).toDouble
          (index, value)
        }.distinct.sortBy(_._1).unzip

        // check if indices are one-based and in ascending order
        var previous = -1
        var i = 0
        val indicesLength = indices.length
        while (i < indicesLength) {
          val current = indices(i)
          require(current > previous, "indices should be one-based and in ascending order")
          previous = current
          i += 1
        }

        (label, indices.toArray, values.toArray)
      }

    // Determine number of features.
    val d = if (numFeatures > 0) {
      numFeatures
    } else {
      parsed.persist(StorageLevel.MEMORY_ONLY)
      parsed.map { case (label, indices, values) =>
        indices.lastOption.getOrElse(0)
      }.reduce(math.max) + 1
    }

    parsed.map { case (label, indices, values) =>
      LabeledPoint(label, Vectors.sparse(d, indices, values))
    }
  }


  def loadLibSVMFilePred(
                      sc: SparkContext,
                      path: String,
                      numFeatures: Int,
                      minPartitions: Int): RDD[(String, String, MLLabeledPoint)] = {
    val parsed = sc.textFile(path, minPartitions)
      .map(_.trim)
      .filter(line => !(line.isEmpty || line.startsWith("#")))
      .map { line =>
        val items = line.split('\t')
        val label = items.head.split(" ")(0)
        val tdid = items.head.split(" ")(1)
        val (indices, values) = items.tail.filter(_.nonEmpty).mkString("").split(" ").map { item =>
          val indexAndValue = item.split(':')
          val index = indexAndValue(0).toInt - 1// Convert 1-based indices to 0-based.
        val value = indexAndValue(1).toFloat
          (index, value)
        }.distinct.sortBy(_._1).unzip

        // check if indices are one-based and in ascending order
        var previous = -1
        var i = 0
        val indicesLength = indices.length
        while (i < indicesLength) {
          val current = indices(i)
          require(current > previous, "indices should be one-based and in ascending order")
          previous = current
          i += 1
        }

        (label, tdid, indices.toArray, values.toArray)
      }

    // Determine number of features.
    val d = if (numFeatures > 0) {
      numFeatures
    } else {
      parsed.persist(StorageLevel.MEMORY_ONLY)
      parsed.map { case (label, tdid, indices, values) =>
        indices.lastOption.getOrElse(0)
      }.reduce(math.max) + 1
    }

    parsed.map { case (label, tdid, indices, values) =>
      (label, tdid, MLLabeledPoint.fromSparseVector(1f, indices, values))
    }
  }

}