/**
  * Created by Kefu on 9/14/2017.
  */
package HadoopRun.kefuSpark

import org.apache.hadoop.io.Text
import org.apache.hadoop.io.BytesWritable
import java.text.SimpleDateFormat
import scala.util.matching.Regex
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import com.talkingdata.Jupiter.searchLocation
import HadoopRun.HDFS.deleteFile
import com.talkingdata.dmp.etl.feature.Entropy
import com.talkingdata.utils.{FileStream, LibSVM, PathUtil}
import org.apache.spark.rdd.UnionRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import ml.dmlc.xgboost4j.scala.spark.{XGBoost => sparkXGBoost}
import ml.dmlc.xgboost4j.scala.{Booster, DMatrix, XGBoost}
import ml.dmlc.xgboost4j.{LabeledPoint => MLLabeledPoint}

import util.control.Breaks._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object DevSex {
  val label = "/datalab/user/yihong.zhang/guozhengtong/trueGenderAgeAll/cb_gzt_union_gender_u"
  def format(tdid:String) = tdid.toLowerCase().replaceAll("[^0-9a-z]", "")

  def main(args: Array[String]): Unit = {
    val input = "/datalab/Plover/TimeFeature20170601To20170630" //Used to be 20170201To20170725
    val output = "/datalab/user/kefu/gender20170601To20170630"
    val conf = new SparkConf().setAppName("test")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val tdid_label = sqlContext.sparkContext.textFile(label).map{
      line =>
        val spl = line.split(",")
        format(spl(0)) -> spl(1).toInt
    }.collectAsMap()
    val bc_labels = sc.broadcast(tdid_label)
    val data = sqlContext.read.parquet(input).select("tdid", "appTimeSet.app").rdd.flatMap{
      line =>
        val tdid = format(line.getAs[String]("tdid"))
        val apps = line.getAs[Seq[Long]]("app")
        bc_labels.value.get(tdid).map{
          label => label + "\t" + apps.mkString(",") + "\t" + tdid
        }
    }.coalesce(1,true).saveAsTextFile(output)
  }
}

object TDID_Gender{
  val label = "/datalab/user/yihong.zhang/guozhengtong/trueGenderAgeAll/cb_gzt_union_gender_u"
  def format(tdid:String) = tdid.toLowerCase().replaceAll("[^0-9a-z]", "")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.driver.maxResultSize","2g")
    val sc = new SparkContext(conf)

    val output = "/datalab/user/kefu/gender_groundTruth"

    val tdid_label = sc.textFile(label).map{
      line =>
        val spl = line.split(",")
        format(spl(0)) + "\t" + spl(1).toInt
    }.saveAsTextFile(output)
  }
}

object wifipixLabled{

  val labelPath = "/datalab/user/kefu/gender20170201To20170725"
  val wifipixPath = "/data/datacenter/aoiPoi/wifipix/2017/*/*" // First extraction was from 20170823-20170922
  val storeNamePath = "/datascience/data/data_3rd/wifipix/poi_brand/poiaoi_20170821.txt"

  def main(args: Array[String]): Unit = {
    val output = "/datalab/user/kefu/wifipixLabelName20170201To20170725"
    val conf = new SparkConf().setAppName("wifipix")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val tdid_label = sc.textFile(labelPath).map{
      line =>
        val col = line.split('\t')
        val sex = col(0)
        val tdid = col(2)
        tdid -> sex
    }.collectAsMap()

    val bc_label = sc.broadcast(tdid_label)

    val wifipix = sc.textFile(wifipixPath).map{
      line =>
        val spl = line.split("\t")
        val tdid = spl(0) //TDID
      val poi = spl(4).replaceAll("[\\[\\]]", "").split(",").toSet //Matchpoi
      val aoi = spl(6).replaceAll("[\\[\\]]", "").split(",").toSet //Matchaoi
      val matchLocation = (poi ++ aoi).filter(_.length > 0)
        tdid -> matchLocation
    }.filter{
      case(tdid, matchLocation) => matchLocation.nonEmpty
    }.reduceByKey((x,y) => x++y)

    val actualwid = wifipix.flatMap{
      case(tdid,wid) => wid
    }.distinct.collect.toSet

    val storeNameMap = sqlContext.read.json(storeNamePath)
      .select("wid","name").rdd.map{
      line =>
        val wid = line.getString(0)
        val name = line.getString(1)
        wid -> name
    }.filter{
      case(wid,name) =>
        actualwid.contains(wid)
    }.collectAsMap()

    val wifipixLabel = wifipix.flatMap{
      case(tdid, matchLocation) =>
        bc_label.value.get(tdid).map{
          label => (label,tdid,matchLocation) // Get the sex label from "bc_label"
        }
    }

    val data = wifipixLabel.map{
      case(label,tdid,matchLocation) =>
        val nameList = ListBuffer[String]() // Initialize empty name list
        for(loc <- matchLocation){
          nameList += storeNameMap getOrElse(loc,"Unknown") //for each wid value, find its name from bc_name and append to nameList
        }
        label + "\t" + nameList.toSet + "\t" + tdid  // Write to line
    }.coalesce(1,true).saveAsTextFile(output) // Write data to file
  }
}

object storeNameEntropy{
  def main(args: Array[String]): Unit = {
    val input = "/datalab/user/kefu/wifipixLabelNameFlat0201_0725"
    val EntropyOutput = "/datalab/user/kefu/storeNameEntropy"
    val RankOutput = "/datalab/user/kefu/storeNameRank"

    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val data = sc.textFile(input)
    val sampleMap = data.map {f =>
      f.split("\t")(2)
    }.countByValue()
    val pos = sampleMap.getOrElse("1", 3l)
    val neg = sampleMap.getOrElse("0", 3l)

    val entropy = Entropy.Entropy(data,pos,neg)

//    if(PathUtil.isExisted(sc,EntropyOutput)){
//      PathUtil.deleteExistedPath(sc,EntropyOutput)
//    }
//    if(PathUtil.isExisted(sc,RankOutput)){
//      PathUtil.deleteExistedPath(sc,RankOutput)
//    }

    entropy.map{f=>
      s"${f._1}\t${f._2}\t${f._3}"
    }.coalesce(1,true).saveAsTextFile(EntropyOutput)

    entropy.map(_._1).zipWithIndex().map{case(name, rank) =>
      s"$name,${rank+1}"
    }.coalesce(1,true).saveAsTextFile(RankOutput)
  }
}

object wifipix{

  val wifipixPath = "/data/datacenter/aoiPoi/wifipix/2017/3[4-8]/*" // First extraction was from 20170823-20170922
  val storeNamePath = "/datascience/data/data_3rd/wifipix/poi_brand/poiaoi_20170821.txt"

  def main(args: Array[String]): Unit = {
    val output = "/datalab/user/kefu/wifipixName20170823-20170922" //20170823-20170922
    val conf = new SparkConf().setAppName("wifipix")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val wifipix = sc.textFile(wifipixPath).map{
      line =>
        val spl = line.split("\t")
        val tdid = spl(0) //TDID
      val poi = spl(4).replaceAll("[\\[\\]]", "").split(",").toSet //Matchpoi
      val aoi = spl(6).replaceAll("[\\[\\]]", "").split(",").toSet //Matchaoi
      val matchLocation = (poi ++ aoi).filter(_.length > 0)
        tdid -> matchLocation
    }.filter{
      case(tdid, matchLocation) => matchLocation.nonEmpty
    }.reduceByKey((x,y) => x++y)

    val actualwid = wifipix.flatMap{
      case(tdid,wid) => wid
    }.distinct.collect.toSet

    val storeNameMap = sqlContext.read.json(storeNamePath)
      .select("wid","name").rdd.map{
      line =>
        val wid = line.getString(0)
        val name = line.getString(1)
        wid -> name
    }.filter{
      case(wid,name) =>
        actualwid.contains(wid)
    }.collectAsMap()

    deleteFile(output)

    val data = wifipix.map{
      case(tdid,matchLocation) =>
        val nameList = ListBuffer[String]() // Initialize empty name list
        for(loc <- matchLocation){
          nameList += storeNameMap getOrElse(loc,"Unknown") //for each wid value, find its name from bc_name and append to nameList
        }
        tdid + "\t" + nameList.toSet  // Write to line
    }.coalesce(1,true).saveAsTextFile(output) // Write data to file
  }
}


object devTypeANDappCat{

  def format(tdid:String) = tdid.toLowerCase().replaceAll("[^0-9a-z]", "")

  def deleteFile(path: String) : Unit = {
    val hdfs: FileSystem = FileSystem.get(new Configuration)
    val isExist = hdfs.exists(new Path(path))
    if (isExist) {
      hdfs.delete(new Path(path), true) //true: delete files recursively
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("devTypeANDappCat")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val output = "/datalab/user/kefu/deviceTypeANDappCategory_labeled"

    deleteFile(output)

    val hashMapMetaID = sqlContext.read.parquet("/datascience/datacloud/datagroup/data/dekun/sql_to_dc/hash_rel_dc/*")
      .select("metaid","hash").rdd.map{
      line =>
        val metaid = line.getAs[Int]("metaid")
        val hash = line.getAs[String]("hash")
        hash -> metaid
    }.collectAsMap()

    val metaIDMapTypeID = sqlContext.read.parquet("/datascience/datacloud/datagroup/data/xiaoyu/app/app_type_2/*")
      .select("metaid","typeid").rdd.map{
      line =>
        val metaid = line.getAs[Int]("metaid")
        val typeid = line.getAs[Int]("typeid")
        metaid -> typeid
    }.collectAsMap()

    val typeIDMapCategory = sqlContext.read.parquet("/datascience/datacloud/datagroup/data/dekun/sql_to_dc/type_dc/*")
      .select("id","name_cn").rdd.map{
      line =>
        val typeid = line.getAs[Int]("id")
        val category = line.getAs[String]("name_cn")
        typeid -> category
    }.collectAsMap()

    val deviceTypeMap = sqlContext.read.parquet("/datascience/datacloud/datagroup/bigtask/device/deivce_info_standard_pro1").rdd.map{
      line =>
        line.getAs[String]("origin_model") -> line.getAs[String]("function_type")
    }.collectAsMap()

    val tdid_label = sc.textFile("/datalab/user/yihong.zhang/guozhengtong/trueGenderAgeAll/cb_gzt_union_gender_u").map{
      line =>
        val col = line.split(",")
        val tdid = format(col(0))
        val label = col(1)
        tdid -> label
    }.collectAsMap()

    val bc_label = sc.broadcast(tdid_label)

    val dataRDD = sqlContext.read.parquet("/datalab/Plover/TimeFeature20170601To20170630")
      .select("tdid","idBox.model","appTimeSet.app").rdd.map{
      line =>
        val tdid = format(line.getAs[String]("tdid"))
        val model = line.getAs[Seq[String]]("model").toSet
        val app = line.getAs[Seq[Long]]("app").toSet[Long].map{
          x => x.toString
        }
        (tdid,model,app)
    }

    val data = dataRDD.map{
      case(tdid, model, apphash) =>
        // Cet device type (origin_model -> function_type)
        val deviceType = ListBuffer[String]()
        for(m <- model){
          deviceType += (deviceTypeMap getOrElse (m,"Unknown")).toString
        }
        // Get app category (hash -> metaid -> typeid -> App分类)
        val appCategory = ListBuffer[String]()
        for(app <- apphash){
          val metaID = Integer.valueOf((hashMapMetaID getOrElse (app,"-999")).toString)
          val typeID = Integer.valueOf((metaIDMapTypeID getOrElse (metaID,"-999")).toString)
          val category = typeIDMapCategory getOrElse (typeID,"UnknownCategory")
          appCategory += category
        }
        // Get the sex label for each tdid
        val label = bc_label.value.get(tdid)
        // Remove Some(),None value from label, remove duplicates in deviceType and appCategory
        (label.toString.replaceAll("[^0-9]", ""),tdid,deviceType.toSet,appCategory.toSet)
    }.filter{
      case(label,tdid,deviceType,appCategory) => label.nonEmpty
    }.map{
      case(label,tdid,deviceType,appCategory) =>
        label + "\t" + tdid + "\t" + deviceType.toSet + "\t" + appCategory.toSet
    }.coalesce(1,true).saveAsTextFile(output)
  }
}

//object getLocation{
//  // @poly 由多边形构成的点（顺时针或者逆时针均可) ArrayBuffer[(lat, lng)]
//  // @pt   待判断的点 (lat, lng)
//  def isPolyContainsPt(poly: Array[(Double, Double)], pt: (Double, Double)): Boolean = {
//    var ret = false
//    var latMin = 90.0
//    var latMax = -90.0
//    var lngMin = 180.0
//    var lngMax = -180.0
//    poly.foreach { pt =>
//      if (pt._1 > latMax) latMax = pt._1
//      if (pt._1 < latMin) latMin = pt._1
//      if (pt._2 > lngMax) lngMax = pt._2
//      if (pt._2 < lngMin) lngMin = pt._2
//    }
//    if (!(pt._1 < latMin || pt._1 > latMax || pt._2 < lngMin || pt._2 > lngMax r)) {
//      for (i <- poly.indices) {
//        val j = (i + 1) % poly.length
//        if ((poly(i)._1 < pt._1) != (poly(j)._1 < pt._1) &&
//          (pt._2 < (poly(j)._2 - poly(i)._2) * (pt._1 - poly(i)._1) / (poly(j)._1 - poly(i)._1) + poly(i)._2)) {
//          ret = !ret
//        }
//      }
//    }
//    ret
//  }
//
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("devTypeANDappCat")
//    val sc = new SparkContext(conf)
//    val sqlContext = new SQLContext(sc)
//
//    val wifipixPath = "/data/datacenter/aoiPoi/wifipix/2017/35/2017-09-01"
//    val output = "/datalab/user/kefu/wifipixCity0901"
//
//    //Geographic fence for city
//    val geoFence = sc.textFile("/data/datacenter/city/codeCityPoly2016-03-25.txt").map{
//      line =>
//        val col = line.split('\t')
//        val area = col(1)
//        val polyFence = col(2).split(';')
//        val poly = ArrayBuffer[(Double,Double)]()
//        for(point <- polyFence){
//          val coordinate = point.split(",")
//          val lat = coordinate(0).toDouble
//          val lng = coordinate(1).toDouble
//          val pt = (lat,lng)
//          poly += pt
//        }
//        area -> poly.toArray
//    }.collectAsMap()
//
//    val wifipixCity = sc.textFile(wifipixPath).map{
//      line =>
//        val spl = line.split("\t")
//        val tdid = spl(0) //TDID
//        val lng = spl(2).toDouble
//        val lat = spl(3).toDouble
//        val poi = spl(4).replaceAll("[\\[\\]]", "").split(",").toSet //Matchpoi
//        val aoi = spl(6).replaceAll("[\\[\\]]", "").split(",").toSet //Matchaoi
//        val matchLocation = (poi ++ aoi).filter(_.length > 0)
//        (tdid,matchLocation,lat,lng)
//    }.filter{
//      case(tdid, matchLocation, lat, lng) => matchLocation.nonEmpty
//    }.map{
//      case(tdid, matchLocation, lat, lng) =>
//        val pt = (lat,lng)
//        val cityList = ArrayBuffer[String]()
//        for(x<-geoFence.keys){
//          val boolean = isPolyContainsPt(geoFence getOrElse(x,Array((0.0,0.0))),pt)
//          if(boolean){
//            cityList += x
//          }
//        }
//        tdid->cityList
//    }.reduceByKey((x,y) => x++y).map{
//      case(tdid,cityList) => tdid + "\t" + cityList
//    }.coalesce(1,true).saveAsTextFile(output)
//  }
//}

object getLocationNew{

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("getLocationNew")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val cityPath = "/data/datacenter/city/codeCityPoly2016-03-25.txt"
    val wifipixPath = "/data/datacenter/aoiPoi/wifipix/2017/3[4-8]/*" //20170823-20170922
    val output = "/datalab/user/kefu/wifipixCity0823-0922"

    val cityEntiy = searchLocation.getCodeGeoEnity(cityPath, sc)
    val (cityRTree, cityAreaMap) = (cityEntiy._1, cityEntiy._2)

    val geoIDMapName = sc.textFile("/data/datacenter/city/codeCityPoly2016-03-25.txt").map{
      line =>
        val col = line.split('\t')
        val code = col(0)
        val areaName = col(1)
        code -> areaName
    }.collectAsMap()

//    val locationSet = Set((32.131741,118.948207), (32.132491380907595,118.94798638299106),
//      (32.13178096,118.94851164), (32.131688,118.948181), (32.13177,118.948217), (32.131745,118.948207),
//      (32.1319620218128,118.94755597226322), (32.131756,118.948211), (32.131751,118.948207), (32.131762,118.948215),
//      (32.13197673205286,118.94749931059779))
//    val cityList = ListBuffer[String]()
//    for(pt <- locationSet){
//      val cityID = searchLocation.rtreeGetCode(pt, cityRTree, cityAreaMap)
//      cityList += geoIDMapName.getOrElse(cityID,"Not Found")
//    }
//
//    sc.parallelize(cityList.toList).coalesce(1,true).saveAsTextFile("/datalab/user/kefu/wifipixCityGeoDebug")

    val wifipixCity = sc.textFile(wifipixPath).map{
      line =>
        val spl = line.split("\t")
        val tdid = spl(0) //TDID
      val lng = spl(2).toDouble
        val lat = spl(3).toDouble
        val poi = spl(4).replaceAll("[\\[\\]]", "").split(",").toSet //Matchpoi
      val aoi = spl(6).replaceAll("[\\[\\]]", "").split(",").toSet //Matchaoi
      val matchLocation = (poi ++ aoi).filter(_.length > 0)
        (tdid,matchLocation,lat,lng)
    }.filter{
      case(tdid, matchLocation, lat, lng) => matchLocation.nonEmpty
    }.map{
      case(tdid, matchLocation, lat, lng) => tdid -> Set((lat,lng))
    }.reduceByKey((x,y)=>x++y).map{
      case(tdid, location) =>
          val cityList = ListBuffer[String]()
          for(pt <- location){
            val cityID = searchLocation.rtreeGetCode(pt, cityRTree, cityAreaMap)
            cityList += geoIDMapName.getOrElse(cityID,"Not Found")
          }
          tdid + "\t" + cityList.toSet
    }.coalesce(1,true).saveAsTextFile(output)
  }
}

object advertising{

  val femaleCampaign = Set("cwJ5oo","QSiDYq8w","FnHvynev","8ztpFE99","dYw8vmKi")
  val output = "/datalab/user/kefu/adSex_201704-201708"

  def format(tdid:String) = tdid.toLowerCase().replaceAll("[^0-9a-z]", "")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("advertising")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // Map TDID to sex label
    val tdid_label = sc.textFile("/datalab/user/kefu/gender20170201To20170725").map{
      line =>
        val col = line.split('\t')
        val sex = col(0)
        val tdid = col(2)
        tdid -> sex
    }.collectAsMap()

    //Map TDID to advertising campaign
    val adMap = sc.textFile("/mcloud/data/datacenter/tdfrontend/feedbacklog/prod/2017*/*").map{ //201704-201708
      line =>
        try{
          val col = line.split(",")
          val bc = col(2).split(":")(1)
          val sc = col(3).split(":")(1)
          val feedBack = col(4)
          val tdid = format(col(6).split(":")(1))
          val matched = col(7)
          (tdid,bc,sc,feedBack,matched)
        } catch {
          case e:Exception => ("None","None","None","None","None")
        }
    }.filter{
      case (tdid,bc,sc,feedBack,matched) =>
        tdid != "None"
    }.map{
      case (tdid,bc,sc,feedBack,matched) =>
        tdid -> Set(bc,sc)
    }.reduceByKey((x,y)=> x++y)

    // Get the TDID in female ads audience
    val femaleAd = adMap.filter{
      case(tdid,bc) =>
        var boolean = false
        for(c <- bc){
          val contain = femaleCampaign.contains(c)
          if(contain){
            boolean = true
          }
        }
        boolean
    }.map{
      case(tdid,bc) =>
        val label = tdid_label.getOrElse(tdid,"Unknown")
        (tdid,label)
    }.filter{
      case(tdid,label) =>
        label != "Unknown"
    }

    // Delete existing file
    deleteFile(output)

    // Get sex lable of TDID which was advertised in female ads
   femaleAd.map{
      case(tdid,label) =>
        label + "\t" + tdid
    }.coalesce(1,true).saveAsTextFile(output)
  }
}

object getPrediction{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val input = Array(
      "/datalab/gender/gender_prediction/aoi/aoi_2017-07",
      "/datalab/gender/gender_prediction/pkg/pkg_2017-07",
      "/datalab/gender/gender_prediction/device/device_2017-07",
      "/datalab/gender/gender_prediction/app/app_2017-07",
      "/datalab/gender/gender_prediction/activity/activity_2017-07")
    val partition = 1000

    val TD_label_prediction = "/datalab/user/kefu/newprediction_TDlabel_07"
    val JD_label_prediction = "/datalab/user/kefu/prediction_JD_07"
    val prediction_output = "/datalab/user/kefu/prediction_07"

    val tdid_label = sc.textFile("/datalab/user/kefu/gender20170201To20170725").map{
      line =>
        val col = line.split('\t')
        val sex = col(0)
        val tdid = col(2)
        tdid -> sex
    }.collectAsMap()

//    val JD_label = sc.textFile("/datalab/user/kefu/JD_new_1w").map{
//      line =>
//        val col = line.split("\t")
//        val tdid = col(0)
//        val label = col(1)
//        tdid -> label
//    }.collectAsMap()

    val data = new UnionRDD(sc,input.map{path =>
      sc.textFile(path).flatMap{line =>
        try{
          Some(line.split("\t")(0) -> ArrayBuffer(line.split("\t")(1).toFloat))
        } catch {
          case e:Exception => None
        }
      }
    }).reduceByKey({
      (m1,m2) => m1 ++ m2
    },partition).flatMap{case (tdid, feature)=>
      try{
        val missingFeature = input.length - feature.length
        val max = feature.max
        val min = feature.min
        Some((tdid,max,min,missingFeature))
      } catch {
        case e:Exception => None
      }
    }

//    val prediction = data.map{
//      case(tdid,max,min)=>
//        tdid + "\t" + max + "\t" + min
//    }.coalesce(1,true).saveAsTextFile(prediction_output)

    val prediction_TDlabel = data.map{
      case(tdid,max,min,missingFeature) =>
        val label = tdid_label.getOrElse(tdid,"Unknown")
        (label,tdid,max,min,missingFeature)
    }.filter{
      case(label,tdid,max,min,missingFeature) =>
        label != "Unknown"
    }.map{
      case(label,tdid,max,min,missingFeature) =>
        label + "\t" + tdid + "\t" + max + "\t" + min + "\t" + missingFeature
    }.coalesce(1,true).saveAsTextFile(TD_label_prediction)

//    val prediction_JDlabel = data.map{
//      case(tdid,max,min) =>
//        val label = JD_label.getOrElse(tdid,"Unknown")
//        (label,tdid,max,min)
//    }.filter{
//      case(label,tdid,max,min) => label != "Unknown"
//    }.map{
//      case(label,tdid,max,min) =>
//        label + "\t" + tdid + "\t" + max + "\t" + min
//    }.coalesce(1,true).saveAsTextFile(JD_label_prediction)

  }
}

object TestModel{

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val output = "/datalab/user/kefu/testModel"

    val data = sc.textFile("/datalab/user/kefu/prediction_TDlabel_07/part-00000").map{
      line =>
        val col = line.replaceAll("[\\(\\)]", "").split(",")
        val label = col(0).toDouble
        val tdid = col(1)
        val max = col(2).toDouble
        val min = col(3).toDouble
        (label, tdid, max, min)
    }

    val trainRDD = sc.parallelize(data.take(100)).map{
      case(label, tdid, max, min) =>
        LabeledPoint(label, Vectors.dense(max,min))
    }

    val numRound = 1000
    val workers = 40
    val paramMap = List("objective" -> "binary:logistic").toMap
    val model = sparkXGBoost.train(trainRDD, paramMap, numRound, nWorkers = workers, useExternalMemory = true)

    model.booster.saveModel(FileStream.getHDFSOutputStream(output))

  }
}

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

object deviceName{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.driver.maxResultSize","2g")
    val sc = new SparkContext(conf)

    val pattern_1 = """("tid":"([^,])*")""".r
    val pattern_2 = """("deviceName":"([^,])*")""".r

//    val input = "/datascience/data/data-writer/ta/standard/data/2017/0[6-7]/*/*"
    val input = "/datascience/data/data-writer/ta/standard/data/2017/07/01/*"
    val labelPath = "/datalab/user/kefu/gender_groundTruth"

//    val output_1 = "/datalab/user/kefu/deviceName_20days"
    val output_2 = "/datalab/user/kefu/tmp"

//    if(PathUtil.isExisted(sc,output_1)){
//      PathUtil.deleteExistedPath(sc,output_1)
//    }

    if(PathUtil.isExisted(sc,output_2)){
      PathUtil.deleteExistedPath(sc,output_2)
    }

    val tdid_label = sc.textFile(labelPath).map{
      line =>
        val col = line.split('\t')
        val sex = col(1)
        val tdid = col(0)
        tdid -> sex
    }.collectAsMap()

    val data = sc.sequenceFile[Text,BytesWritable](input).map{
      case(key,value) =>
        val len = value.getLength
        val index = value.getBytes.indexOf('\0'.toByte)
        val headerBytes = value.getBytes.slice(0, index)
        val bodyBytes = value.getBytes.slice(index + 1, len)

        val bodyString = new String(bodyBytes)
        // Case: 9D6FF330-DFAA-4354-9B93-7C8ED8317720
        // Solution: toLowerCase(),replaceAll([-],"")
        val tdid = (pattern_1 findAllIn bodyString).mkString(",").split(",").distinct.mkString("").toLowerCase().replaceAll("""[":-]|(tid)""","")
        val deviceName = (pattern_2 findAllIn bodyString).mkString(",").split(",").distinct.mkString(",").replaceAll("""[":]|(deviceName)""","")

        val label = tdid_label.getOrElse(tdid,"Unknown")
        (label,tdid,deviceName)
    }

//    data.filter{
//      case(label,tdid,deviceName) => (deviceName != "")
//    }.map{
//      case(label,tdid,deviceName) =>
//        label + "\t" + tdid + "\t" + deviceName
//    }.distinct.saveAsTextFile(output_1)

    data.filter{
      case(label,tdid,deviceName) => (deviceName != "") & (label != "Unknown")
    }.map{
      case(label,tdid,deviceName) =>
        label + "\t" + tdid + "\t" + deviceName
    }.distinct.saveAsTextFile(output_2)
  }
}

object agedata_step1{

  def path(year:String,month:String,base:String)={
    var path = ""
    val basePath = base + "/" + year + "/" + month
    for(day <- 1 to 30){
      var newday = ""
      if(day < 10){
        newday = "0" + day
      } else{
        newday = day.toString
      }
      if(path == ""){
        path = basePath + "/" + newday + ","
      } else{
        path = path + basePath + "/" + newday + ","
      }
    }
    path.split(",")
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.driver.maxResultSize", "2g")
    conf.set("spark.akka.frameSize","100")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)


    val year = args(0)
    val month = args(1)
    val base = "/datascience/etl2/aggregate/ta"

    val output = "/datalab/user/kefu/extractData_ta_" + year + "_" + month

    if(PathUtil.isExisted(sc,output)){
      PathUtil.deleteExistedPath(sc,output)
    }

    val data = new UnionRDD(sc, path(year, month, base).map {
      input =>
        sqlContext.read.parquet(input).
          select("deviceId", "seq.appkey", "seq.simOperator", "info.imei").rdd.map {
          line =>
            val tdid = line.getAs[String]("deviceId")
            val appkey = line.getAs[Seq[String]]("appkey").mkString(",").split(",").distinct.mkString("")
            val mcc_mnc = line.getAs[Seq[String]]("simOperator").mkString(",").replaceAll("(WrappedArray)|[()]", "").split(",").distinct.mkString("")
            val imei = line.getAs[Seq[String]]("imei").mkString(",").replaceAll("(WrappedArray)|[()]", "").split(",").distinct.mkString("")
            // (tdid,appkey,mcc_mnc,imei)
            tdid -> (Set(appkey), Set(mcc_mnc), Set(imei))
        }
    }).reduceByKey((x1, x2) => (x1._1 ++ x2._1, x1._2 ++ x2._2, x1._3 ++ x2._3))

    data.map{
      case(key,(appkey,mcc_mnc,imei)) =>
        key + "\t" + appkey.mkString(",") + "\t" + mcc_mnc.mkString(",") + "\t" + imei.mkString(",")
    }.saveAsTextFile(output)
  }
}

object agedata_step2{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.set("spark.akka.frameSize","1000")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val inputList = ("/datalab/user/kefu/extractData_ta_2017_05," +
      "/datalab/user/kefu/extractData_ta_2017_06," +
      "/datalab/user/kefu/extractData_ta_2017_07," +
      "/datalab/user/kefu/extractData_ta_2017_10," +
      "/datalab/user/kefu/extractData_ta_2017_11," +
      "/datalab/user/kefu/extractData_ta_2017_12").split(",")

//    val inputList = ("/datalab/user/kefu/extractData_ta_2017_10," +
//      "/datalab/user/kefu/extractData_ta_2017_11," +
//      "/datalab/user/kefu/extractData_ta_2017_12").split(",")

    val output = "/datalab/user/kefu/extract_LianTong_6months"
//    val output = "/datalab/user/kefu/extract_LianTong_3months_2"

    if(PathUtil.isExisted(sc,output)){
      PathUtil.deleteExistedPath(sc,output)
    }

    val data = new UnionRDD(sc,inputList.map{
      input => sc.textFile(input).map{
        line =>
          var col = line.split("\t")
          var tdid = "None"
          var appkey = "None"
          var mcc_mnc = "None"
          var imei = "None"
          try{
            tdid = col(0)
          } catch {
            case e:Exception => tdid = "None"
          }
          try{
            appkey = col(1)
          } catch {
            case e:Exception => appkey = "None"
          }
          try{
            mcc_mnc = col(2)
          } catch {
            case e:Exception => mcc_mnc = "None"
          }
          try{
            imei = col(3)
          } catch {
            case e:Exception => imei = "None"
          }
          tdid -> (appkey.split(",").toSet,mcc_mnc.split(",").toSet,imei.split(",").toSet)
      }
    }).filter{
      case(key,(appkey,mcc_mnc,imei))=>((appkey.mkString("") != "") & (appkey.mkString("") != "None")) &
        ((mcc_mnc.mkString("") != "") & (mcc_mnc.mkString("") != "None")) &
        ((imei.mkString("") != "") & (imei.mkString("") != "None"))
    }.filter{
      case(key,(appkey,mcc_mnc,imei)) =>
        // 联通mcc_mnc:46001,46006
        mcc_mnc.contains("46001")|mcc_mnc.contains("46006")
    }.filter{
      case(key,(appkey,mcc_mnc,imei)) =>
        imei.toList.length < 100
    }.reduceByKey((x1,x2)=>(x1._1 ++ x2._1, x1._2 ++ x2._2, x1._3 ++ x2._3),5000)

    data.map{
      case(key,(appkey,mcc_mnc,imei)) =>
        key + "\t" + appkey.mkString(",") + "\t" + mcc_mnc.mkString(",") + "\t" + imei.mkString(",")
    }.saveAsTextFile(output)
  }
}

object district{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.driver.maxResultSize", "2g")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val output = "/datalab/user/kefu/extractData_district"

    if(PathUtil.isExisted(sc,output)){
      PathUtil.deleteExistedPath(sc,output)
    }

    val path = "/user/jun.zhu/PopulationStatBJ2/DistributionResult2/2017-05/StablePopulation/tdid/district," +
      "/user/jun.zhu/PopulationStatBJ2/DistributionResult2/2017-06/StablePopulation/tdid/district," +
      "/user/jun.zhu/PopulationStatBJ2/DistributionResult2/2017-07/StablePopulation/tdid/district," +
      "/user/jun.zhu/PopulationStatBJ2/DistributionResult2/2017-10/StablePopulation/tdid/district," +
      "/user/jun.zhu/PopulationStatBJ2/DistributionResult2/2017-11/StablePopulation/tdid/district," +
      "/user/jun.zhu/PopulationStatBJ2/DistributionResult2/2017-12/StablePopulation/tdid/district"

    val data = new UnionRDD(sc, path.split(",").map {
      input =>
        sqlContext.read.parquet(input).rdd.map{
          line =>
            val tdid = line.getAs[String]("tdid")
            val district = line.getAs[String]("district")
            tdid -> district
        }
    }).reduceByKey((x1,x2)=>x1+","+x2).filter{
      case(tdid,districtList)=>
        val count = districtList.split(",").length
        count == 5
    }.map{
      case(tdid,districtList)=>
        val dList = districtList.split(",")
        var max = 0
        var major = ""
        for(d <- dList.distinct){
          val freq = dList.count(x=>x==d)
          if(freq > max){
            max = freq
            major = d
          }
        }
        (tdid,major,max)
    }

    data.map{
      case(tdid,major,max) =>
        tdid + "\t" + major + "\t" + max
    }.saveAsTextFile(output)
  }
}

object agedata_merge{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.driver.maxResultSize", "2g")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val output = "/datalab/user/kefu/ageData_merge"

    val input_1 = sc.textFile("/datalab/user/kefu/extractData_district").map{
      line =>
        val col = line.split("\t")
        val tdid = col.head
        val feature = col.tail.mkString("\t")
        tdid -> feature
    }
    val input_2 = sc.textFile("/datalab/user/kefu/extract_LianTong_6months").map{
      line =>
        val col = line.split("\t")
        val tdid = col.head
        val location = col.tail.mkString("\t")
        tdid -> location
    }

    val merge_result = (input_1 join input_2).map{
      case(tdid,(location,ta)) =>
        val district = location.split("\t")(0)
        val max = location.split("\t")(1)
        val appkey = ta.split("\t")(0)
        val mcc_mnc = ta.split("\t")(1)
        val imei = ta.split("\t")(2)
        (tdid,district,max,appkey,mcc_mnc,imei)
    }

    merge_result.map{
      case(tdid,district,max,appkey,mcc_mnc,imei)=>
        tdid + "\t" + district + "\t" + max + "\t" + appkey + "\t" + mcc_mnc + "\t" + imei
    }.repartition(1000).saveAsTextFile(output)
  }
}

object agedata_final{

  def splitWrite(input:RDD[String], baseName:String, weights:Array[Double], seed:Long, sc:SparkContext)={
    val input_split = input.randomSplit(weights,seed)

    var count = 1

    for(i <- input_split){
      val out = "/datalab/user/kefu/ageData/batches/"+count.toString+"/"+baseName
      if(PathUtil.isExisted(sc,out)){
        PathUtil.deleteExistedPath(sc,out)
      }
      i.repartition(50).saveAsTextFile(out)
      count += 1
    }
  }

  def subset(input:RDD[String]) ={
    val input_filter = input.filter{
      line =>
        val col = line.split("\t")
        val appKey = col(3)
        val numApp = appKey.split(",").length
        numApp >= 3
    }

    input_filter
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.driver.maxResultSize", "2g")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val seed = 12345.toLong
    val weights = Array(0.2,0.2,0.2,0.2,0.2)

    val ChangPing = sc.textFile("/datalab/user/kefu/ageData/batches/rest/ChangPing")
    val FangShan = sc.textFile("/datalab/user/kefu/ageData/batches/rest/FangShan")
    val PingGu = sc.textFile("/datalab/user/kefu/ageData/batches/rest/PingGu")
    val MenTouGou = sc.textFile("/datalab/user/kefu/ageData/batches/rest/MenTouGou")
    val ShunYi = sc.textFile("/datalab/user/kefu/ageData/batches/rest/ShunYi")
    val DongCheng = sc.textFile("/datalab/user/kefu/ageData/batches/rest/DongCheng")
    val XiCheng = sc.textFile("/datalab/user/kefu/ageData/batches/rest/XiCheng")
    val HaiDian = sc.textFile("/datalab/user/kefu/ageData/batches/rest/HaiDian")
    val ShiJingShan = sc.textFile("/datalab/user/kefu/ageData/batches/rest/ShiJingShan")
    val YanQing = sc.textFile("/datalab/user/kefu/ageData/batches/rest/YanQing")
    val TongZhou = sc.textFile("/datalab/user/kefu/ageData/batches/rest/TongZhou")
    val DaXing = sc.textFile("/datalab/user/kefu/ageData/batches/rest/DaXing")
    val MiYun = sc.textFile("/datalab/user/kefu/ageData/batches/rest/MiYun")
    val ChaoYang = sc.textFile("/datalab/user/kefu/ageData/batches/rest/ChaoYang")
    val FengTai = sc.textFile("/datalab/user/kefu/ageData/batches/rest/FengTai")
    val HuaiRou = sc.textFile("/datalab/user/kefu/ageData/batches/rest/HuaiRou")




    val ChangPing_sub = subset(ChangPing)
    splitWrite(ChangPing_sub,"ChangPing",weights,seed,sc)
    val FangShan_sub = subset(FangShan)
    splitWrite(FangShan_sub,"FangShan",weights,seed,sc)
    val PingGu_sub = subset(PingGu)
    splitWrite(PingGu_sub,"PingGu",weights,seed,sc)
    val MenTouGou_sub = subset(MenTouGou)
    splitWrite(MenTouGou_sub,"MenTouGou",weights,seed,sc)
    val ShunYi_sub = subset(ShunYi)
    splitWrite(ShunYi_sub,"ShunYi",weights,seed,sc)
    val DongCheng_sub = subset(DongCheng)
    splitWrite(DongCheng_sub,"DongCheng",weights,seed,sc)
    val XiCheng_sub = subset(XiCheng)
    splitWrite(XiCheng_sub,"XiCheng",weights,seed,sc)
    val HaiDian_sub = subset(HaiDian)
    splitWrite(HaiDian_sub,"HaiDian",weights,seed,sc)
    val ShiJingShan_sub = subset(ShiJingShan)
    splitWrite(ShiJingShan_sub,"ShiJingShan",weights,seed,sc)
    val YanQing_sub = subset(YanQing)
    splitWrite(YanQing_sub,"YanQing",weights,seed,sc)
    val TongZhou_sub = subset(TongZhou)
    splitWrite(TongZhou_sub,"TongZhou",weights,seed,sc)
    val DaXing_sub = subset(DaXing)
    splitWrite(DaXing_sub,"DaXing",weights,seed,sc)
    val MiYun_sub = subset(MiYun)
    splitWrite(MiYun_sub,"MiYun",weights,seed,sc)
    val ChaoYang_sub = subset(ChaoYang)
    splitWrite(ChaoYang_sub,"ChaoYang",weights,seed,sc)
    val FengTai_sub = subset(FengTai)
    splitWrite(FengTai_sub,"FengTai",weights,seed,sc)
    val HuaiRou_sub = subset(HuaiRou)
    splitWrite(HuaiRou_sub,"HuaiRou",weights,seed,sc)
  }
}

object tdid_imei{

  // Make sure the tdid is in correct format
  def format(tdid:String) = tdid.toLowerCase().replaceAll("[^0-9a-z]", "")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.driver.maxResultSize","2g")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val inputs = args(0).split(",")
    val outputs = args(1).split(",")

    val fm = new SimpleDateFormat("yyyyMMdd")

    val all = new UnionRDD(sc,inputs.map{
      path => sc.textFile(path).map{
        line => line.split("\t")(1)
      }
    }).distinct.saveAsTextFile("/datalab/user/kefu/ageData/out_batches/1/all")
  }
}

object dbscan_didi{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.driver.maxResultSize","2g")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val output_1 = "/datalab/user/kefu/work_residence/month9_wifiLoc"
    val output_2 = "/datalab/user/kefu/work_residence/month9_join_didi"

    if(PathUtil.isExisted(sc,output_1)){
      PathUtil.deleteExistedPath(sc, output_1)
    }

    if(PathUtil.isExisted(sc,output_2)){
      PathUtil.deleteExistedPath(sc, output_2)
    }

    val company_didi = sc.textFile("/datalab/user/kefu/work_residence/company_location_didi.csv").map{
      line =>
        val col = line.split(",")
        val tdid = col(0)
        val dlng = col(1)
        val dlat = col(2)
        tdid -> (dlng,dlat)
    }

    val data = sqlContext.read.parquet("/datalab/user/frank.zhang/data/wifidata/feature/month9WifiLoc").rdd.map{
      line =>
        val tdid = line.getAs[String]("tdid")
        val ssid = line.getAs[String]("ssid")
        val bssid = line.getAs[Long]("bssid")
        var rlng = line.getAs[Double]("rlng")
        var rlat = line.getAs[Double]("rlat")
        val freq = line.getAs[Int]("freq")
        val duration = line.getAs[Int]("duration")
        var wifiPool = false
        // When the device didn't upload location information, enrich the context using location for the wifi
        // from wifiPool (lng,lat)
        if(rlng.isNaN){
          rlng = line.getAs[Double]("lng")
          wifiPool = true
        }
        if(rlat.isNaN){
          rlat = line.getAs[Double]("lat")
          wifiPool = true
        }
        tdid ->(ssid,bssid,rlng,rlat,freq,duration,wifiPool)
    }

    data.map{
      case(tdid,(ssid,bssid,rlng,rlat,freq,duration,wifiPool)) =>
        (tdid,ssid,bssid,rlng,rlat,freq,duration,wifiPool)
    }.toDF.write.parquet(output_1)

    val join_didi = (data join company_didi).map{
      case(tdid,(ta,didi)) =>
        val ssid = ta._1
        val bssid = ta._2
        val rlng = ta._3
        val rlat = ta._4
        val freq = ta._5
        val duration = ta._6
        val wifiPool = ta._7
        val dlng = didi._1
        val dlat = didi._2
        (tdid,ssid,bssid,rlng,rlat,freq,duration,wifiPool,dlng,dlat)
    }

    join_didi.toDF.write.parquet(output_2)


  }
}

object combine_predict{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.driver.maxResultSize","2g")
    val sc = new SparkContext(conf)

    val male_input = "/datalab/gender/gender_prediction/result/balanced/5_to_5/tmp/2017-11_male_batches_1," +
      "/datalab/gender/gender_prediction/result/balanced/5_to_5/tmp/2017-11_male_batches_2," +
      "/datalab/gender/gender_prediction/result/balanced/5_to_5/tmp/2017-11_male_batches_3," +
      "/datalab/gender/gender_prediction/result/balanced/5_to_5/tmp/2017-11_male_batches_4," +
      "/datalab/gender/gender_prediction/result/balanced/5_to_5/tmp/2017-11_male_batches_5"
    val female_input = "/datalab/gender/gender_prediction/result/balanced/5_to_5/tmp/2017-11_female_batches_1," +
      "/datalab/gender/gender_prediction/result/balanced/5_to_5/tmp/2017-11_female_batches_2," +
      "/datalab/gender/gender_prediction/result/balanced/5_to_5/tmp/2017-11_female_batches_3," +
      "/datalab/gender/gender_prediction/result/balanced/5_to_5/tmp/2017-11_female_batches_4," +
      "/datalab/gender/gender_prediction/result/balanced/5_to_5/tmp/2017-11_female_batches_5"

    val male_output = "/datalab/gender/gender_prediction/result/balanced/5_to_5/male_2017-11"
    val female_output = "/datalab/gender/gender_prediction/result/balanced/5_to_5/female_2017-11"

    if(PathUtil.isExisted(sc,male_output)){
      PathUtil.deleteExistedPath(sc, male_output)
    }
    if(PathUtil.isExisted(sc,female_output)){
      PathUtil.deleteExistedPath(sc, female_output)
    }

    val male = new UnionRDD(sc,male_input.split(",").map{path =>
      sc.textFile(path)})
    val female = new UnionRDD(sc,female_input.split(",").map{path =>
      sc.textFile(path)})

    male.coalesce(512).saveAsTextFile(male_output)
    female.coalesce(512).saveAsTextFile(female_output)
  }
}

object gender_decay{

  def format(tdid:String) = tdid.toLowerCase().replaceAll("[^0-9a-z]", "")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.driver.maxResultSize","2g")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val input = args(0)
    val output = args(1)

    val data = sqlContext.read.parquet(input).select("deviceId").rdd.map{
      line => format(line.getAs[String]("deviceId"))
    }.distinct

    println("This month have " + data.count.toString + " TDID")

    val labelPath = "/datalab/user/kefu/gender_groundTruth"

    val sex_label = sc.textFile(labelPath).map{
      line =>
        val col = line.split('\t')
        val tdid = format(col(0))
        tdid
    }.collect.toSet

    if(PathUtil.isExisted(sc,output)){
      PathUtil.deleteExistedPath(sc, output)
    }

    data.filter{
      line => sex_label.contains(line)
    }.repartition(100).saveAsTextFile(output)
  }
}


object tmp{

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.driver.maxResultSize","2g")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    println("LOL")

  }
}