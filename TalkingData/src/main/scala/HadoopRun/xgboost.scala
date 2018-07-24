package HadoopRun

import com.talkingdata.utils.FileStream
import ml.dmlc.xgboost4j.scala.{Booster, DMatrix, XGBoost}
import ml.dmlc.xgboost4j.{LabeledPoint => MLLabeledPoint}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by Kefu on 12/26/17.
  */
object TestPrediction {

  def predByXg(sc:SparkContext,testSet: RDD[(String, MLLabeledPoint)],model:Booster)={
    val broadcastModel = sc.broadcast(model)
    testSet.map(_._2).mapPartitions{ iter =>
      broadcastModel.value.predict(new DMatrix(iter)).map(_.head).toIterator
    }.zip(testSet).map{case(pred, (tdid, feauture)) =>
      s"$tdid\t$pred"
    }
  }

  def loadModel(input:String) = XGBoost.loadModel(FileStream.getHDFSInputStream(input))

  def main(args: Array[String]): Unit = {
    val conf  =  new SparkConf()
    val sc = new SparkContext(conf)
    val output = "/datalab/user/kefu/testModelPrediction"
    val modelPath = "/datalab/user/kefu/testModel"

    val model = loadModel(modelPath)

    val data = sc.textFile("/datalab/user/kefu/prediction_TDlabel_07/part-00000").map{
      line =>
        val col = line.replaceAll("[\\(\\)]", "").split(",")
        val label = col(0).toFloat
        val tdid = col(1)
        val max = col(2).toFloat
        val min = col(3).toFloat
        (label, tdid, max, min)
    }

    val testRDD = sc.parallelize(data.take(1000)).map{
      case(label, tdid, max, min) =>
        (tdid,MLLabeledPoint.fromDenseVector(label, Array(max, min)))
    }

    val result = predByXg(sc, testRDD, model)

    result.saveAsTextFile(output)
    }
}
