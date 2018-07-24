///**
//  * Created by Kefu on 02/06/2018.
//  */
//
//package localRun
//
//import java.text.SimpleDateFormat
//import com.talkingdata.utils.PathUtil
//import org.apache.spark.rdd.{RDD, UnionRDD}
//import org.apache.spark.sql.SQLContext
//import org.apache.spark.{SparkConf, SparkContext}
//
//// Extract appkey, mcc_mnc and imei information from ta daily aggregated dataset
////
//// Input: year[String], month[String]
//// Output: /datalab/user/kefu/extractData_ta_$year_$month
//object agedata_step1{
//
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().set("spark.driver.maxResultSize", "2g")
//    conf.set("spark.akka.frameSize","100")
//    val sc = new SparkContext(conf)
//    val sqlContext = new SQLContext(sc)
//
//
//    val year = args(0)
//    val month = args(1)
//    val base = "/datascience/etl2/aggregate/ta"
//
//    val output = "/datalab/user/kefu/extractData_ta_" + year + "_" + month
//
//    if(PathUtil.isExisted(sc,output)){
//      PathUtil.deleteExistedPath(sc,output)
//    }
//
//    val data = sqlContext.read.parquet("/datascience/etl2/aggregate/ta/2017/" + month + "/*").
//          select("deviceId", "seq.appkey", "seq.simOperator", "info.imei").rdd.map {
//          line =>
//            val tdid = line.getAs[String]("deviceId")
//            val appkey = line.getAs[Seq[String]]("appkey").mkString(",").split(",").distinct.mkString("")
//            val mcc_mnc = line.getAs[Seq[String]]("simOperator").mkString(",").replaceAll("(WrappedArray)|[()]", "").split(",").distinct.mkString("")
//            val imei = line.getAs[Seq[String]]("imei").mkString(",").replaceAll("(WrappedArray)|[()]", "").split(",").distinct.mkString("")
//            // (tdid,appkey,mcc_mnc,imei)
//            tdid -> (Set(appkey), Set(mcc_mnc), Set(imei))
//        }.reduceByKey((x1, x2) => (x1._1 ++ x2._1, x1._2 ++ x2._2, x1._3 ++ x2._3))
//
//    data.map{
//      case(key,(appkey,mcc_mnc,imei)) =>
//        key + "\t" + appkey.mkString(",") + "\t" + mcc_mnc.mkString(",") + "\t" + imei.mkString(",")
//    }.saveAsTextFile(output)
//  }
//}
//
//// Aggregate 6 months data and filter the dataset based on two conditions:
//// 1) The mcc_mnc code is either 46001 or 46006 (联通)
//// 2) The number of imei code is no more than 100
////
//// Input: /datalab/user/kefu/extractData_ta_2017_05,/datalab/user/kefu/extractData_ta_2017_06,/datalab/user/kefu/extractData_ta_2017_07,
////        /datalab/user/kefu/extractData_ta_2017_10,/datalab/user/kefu/extractData_ta_2017_11,/datalab/user/kefu/extractData_ta_2017_12
//// Output: /datalab/user/kefu/extract_LianTong_6months
//object agedata_step2{
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf()
//    conf.set("spark.akka.frameSize","1000")
//    val sc = new SparkContext(conf)
//    val sqlContext = new SQLContext(sc)
//
//    val inputList = ("/datalab/user/kefu/extractData_ta_2017_05," +
//      "/datalab/user/kefu/extractData_ta_2017_06," +
//      "/datalab/user/kefu/extractData_ta_2017_07," +
//      "/datalab/user/kefu/extractData_ta_2017_10," +
//      "/datalab/user/kefu/extractData_ta_2017_11," +
//      "/datalab/user/kefu/extractData_ta_2017_12").split(",")
//
//    val output = "/datalab/user/kefu/extract_LianTong_6months"
//
//    if(PathUtil.isExisted(sc,output)){
//      PathUtil.deleteExistedPath(sc,output)
//    }
//
//    val data = new UnionRDD(sc,inputList.map{
//      input => sc.textFile(input).map{
//        line =>
//          var col = line.split("\t")
//          var tdid = "None"
//          var appkey = "None"
//          var mcc_mnc = "None"
//          var imei = "None"
//          try{
//            tdid = col(0)
//          } catch {
//            case e:Exception => tdid = "None"
//          }
//          try{
//            appkey = col(1)
//          } catch {
//            case e:Exception => appkey = "None"
//          }
//          try{
//            mcc_mnc = col(2)
//          } catch {
//            case e:Exception => mcc_mnc = "None"
//          }
//          try{
//            imei = col(3)
//          } catch {
//            case e:Exception => imei = "None"
//          }
//          tdid -> (appkey.split(",").toSet,mcc_mnc.split(",").toSet,imei.split(",").toSet)
//      }
//    }).filter{
//      case(key,(appkey,mcc_mnc,imei))=>((appkey.mkString("") != "") & (appkey.mkString("") != "None")) &
//        ((mcc_mnc.mkString("") != "") & (mcc_mnc.mkString("") != "None")) &
//        ((imei.mkString("") != "") & (imei.mkString("") != "None"))
//    }.filter{
//      case(key,(appkey,mcc_mnc,imei)) =>
//        // 联通mcc_mnc:46001,46006
//        mcc_mnc.contains("46001")|mcc_mnc.contains("46006")
//    }.filter{
//      case(key,(appkey,mcc_mnc,imei)) =>
//        imei.toList.length < 100
//    }.reduceByKey((x1,x2)=>(x1._1 ++ x2._1, x1._2 ++ x2._2, x1._3 ++ x2._3),5000)
//
//    data.map{
//      case(key,(appkey,mcc_mnc,imei)) =>
//        key + "\t" + appkey.mkString(",") + "\t" + mcc_mnc.mkString(",") + "\t" + imei.mkString(",")
//    }.saveAsTextFile(output)
//  }
//}
//
//// Find the district that each TDID belongs to in Beijing
////
//// Input: /user/jun.zhu/PopulationStatBJ2/DistributionResult2/2017-05/StablePopulation/tdid/district,
////        /user/jun.zhu/PopulationStatBJ2/DistributionResult2/2017-06/StablePopulation/tdid/district,
////        /user/jun.zhu/PopulationStatBJ2/DistributionResult2/2017-07/StablePopulation/tdid/district,
////        /user/jun.zhu/PopulationStatBJ2/DistributionResult2/2017-10/StablePopulation/tdid/district,
////        /user/jun.zhu/PopulationStatBJ2/DistributionResult2/2017-11/StablePopulation/tdid/district,
////        /user/jun.zhu/PopulationStatBJ2/DistributionResult2/2017-12/StablePopulation/tdid/district
//// Output: /datalab/user/kefu/extractData_district
//object district{
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().set("spark.driver.maxResultSize", "2g")
//    val sc = new SparkContext(conf)
//    val sqlContext = new SQLContext(sc)
//
//    val output = "/datalab/user/kefu/extractData_district"
//
//    if(PathUtil.isExisted(sc,output)){
//      PathUtil.deleteExistedPath(sc,output)
//    }
//
//    val path = "/user/jun.zhu/PopulationStatBJ2/DistributionResult2/2017-05/StablePopulation/tdid/district," +
//      "/user/jun.zhu/PopulationStatBJ2/DistributionResult2/2017-06/StablePopulation/tdid/district," +
//      "/user/jun.zhu/PopulationStatBJ2/DistributionResult2/2017-07/StablePopulation/tdid/district," +
//      "/user/jun.zhu/PopulationStatBJ2/DistributionResult2/2017-10/StablePopulation/tdid/district," +
//      "/user/jun.zhu/PopulationStatBJ2/DistributionResult2/2017-11/StablePopulation/tdid/district," +
//      "/user/jun.zhu/PopulationStatBJ2/DistributionResult2/2017-12/StablePopulation/tdid/district"
//
//    val data = new UnionRDD(sc, path.split(",").map {
//      input =>
//        sqlContext.read.parquet(input).rdd.map{
//          line =>
//            val tdid = line.getAs[String]("tdid")
//            val district = line.getAs[String]("district")
//            tdid -> district
//        }
//    }).reduceByKey((x1,x2)=>x1+","+x2).filter{
//      // Only keep TDID that occurs in every month throughout the 6 months
//      case(tdid,districtList)=>
//        val count = districtList.split(",").length
//        count == 5
//    }.map{
//      case(tdid,districtList)=>
//        val dList = districtList.split(",")
//        var max = 0
//        var major = ""
//        for(d <- dList.distinct){
//          val freq = dList.count(x=>x==d)
//          if(freq > max){
//            max = freq
//            major = d
//          }
//        }
//        (tdid,major,max)
//    }
//
//    data.map{
//      case(tdid,major,max) =>
//        tdid + "\t" + major + "\t" + max
//    }.saveAsTextFile(output)
//  }
//}
//
//// Merge (1) data from ta daily aggregated dataset and (2) district data together
////
//// Input: /datalab/user/kefu/extract_LianTong_6months, /datalab/user/kefu/extractData_district
//// Output: /datalab/user/kefu/ageData_merge
//object agedata_merge{
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().set("spark.driver.maxResultSize", "2g")
//    val sc = new SparkContext(conf)
//    val sqlContext = new SQLContext(sc)
//
//    val output = "/datalab/user/kefu/ageData_merge"
//
//    val input_1 = sc.textFile("/datalab/user/kefu/extractData_district").map{
//      line =>
//        val col = line.split("\t")
//        val tdid = col.head
//        val feature = col.tail.mkString("\t")
//        tdid -> feature
//    }
//    val input_2 = sc.textFile("/datalab/user/kefu/extract_LianTong_6months").map{
//      line =>
//        val col = line.split("\t")
//        val tdid = col.head
//        val location = col.tail.mkString("\t")
//        tdid -> location
//    }
//
//    val merge_result = (input_1 join input_2).map{
//      case(tdid,(location,ta)) =>
//        val district = location.split("\t")(0)
//        val max = location.split("\t")(1)
//        val appkey = ta.split("\t")(0)
//        val mcc_mnc = ta.split("\t")(1)
//        val imei = ta.split("\t")(2)
//        (tdid,district,max,appkey,mcc_mnc,imei)
//    }
//
//    merge_result.map{
//      case(tdid,district,max,appkey,mcc_mnc,imei)=>
//        tdid + "\t" + district + "\t" + max + "\t" + appkey + "\t" + mcc_mnc + "\t" + imei
//    }.repartition(1000).saveAsTextFile(output)
//  }
//}
//
//// Filter the data from 16 different districts:
//// 1) Only keep TDID that has at least 3 host apps
//// Randomly split the dataset from 16 different districts into 5 batches in roughly the same size
////
//// Input: /datalab/user/kefu/ageData/batches/rest/$distict
//// Output: /datalab/user/kefu/ageData/batches/$batch_number/$district
//object agedata_final{
//
//  def splitWrite(input:RDD[String], baseName:String, weights:Array[Double], seed:Long, sc:SparkContext)={
//    val input_split = input.randomSplit(weights,seed)
//
//    var count = 1
//
//    for(i <- input_split){
//      val out = "/datalab/user/kefu/ageData/batches/"+count.toString+"/"+baseName
//      if(PathUtil.isExisted(sc,out)){
//        PathUtil.deleteExistedPath(sc,out)
//      }
//      i.repartition(50).saveAsTextFile(out)
//      count += 1
//    }
//  }
//
//  def subset(input:RDD[String]) ={
//    val input_filter = input.filter{
//      line =>
//        val col = line.split("\t")
//        val appKey = col(3)
//        val numApp = appKey.split(",").length
//        numApp >= 3
//    }
//
//    input_filter
//  }
//
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().set("spark.driver.maxResultSize", "2g")
//    val sc = new SparkContext(conf)
//    val sqlContext = new SQLContext(sc)
//
//    val seed = 12345.toLong
//    val weights = Array(0.2,0.2,0.2,0.2,0.2)
//
//    val ChangPing = sc.textFile("/datalab/user/kefu/ageData/batches/rest/ChangPing")
//    val FangShan = sc.textFile("/datalab/user/kefu/ageData/batches/rest/FangShan")
//    val PingGu = sc.textFile("/datalab/user/kefu/ageData/batches/rest/PingGu")
//    val MenTouGou = sc.textFile("/datalab/user/kefu/ageData/batches/rest/MenTouGou")
//    val ShunYi = sc.textFile("/datalab/user/kefu/ageData/batches/rest/ShunYi")
//    val DongCheng = sc.textFile("/datalab/user/kefu/ageData/batches/rest/DongCheng")
//    val XiCheng = sc.textFile("/datalab/user/kefu/ageData/batches/rest/XiCheng")
//    val HaiDian = sc.textFile("/datalab/user/kefu/ageData/batches/rest/HaiDian")
//    val ShiJingShan = sc.textFile("/datalab/user/kefu/ageData/batches/rest/ShiJingShan")
//    val YanQing = sc.textFile("/datalab/user/kefu/ageData/batches/rest/YanQing")
//    val TongZhou = sc.textFile("/datalab/user/kefu/ageData/batches/rest/TongZhou")
//    val DaXing = sc.textFile("/datalab/user/kefu/ageData/batches/rest/DaXing")
//    val MiYun = sc.textFile("/datalab/user/kefu/ageData/batches/rest/MiYun")
//    val ChaoYang = sc.textFile("/datalab/user/kefu/ageData/batches/rest/ChaoYang")
//    val FengTai = sc.textFile("/datalab/user/kefu/ageData/batches/rest/FengTai")
//    val HuaiRou = sc.textFile("/datalab/user/kefu/ageData/batches/rest/HuaiRou")
//
//
//
//
//    val ChangPing_sub = subset(ChangPing)
//    splitWrite(ChangPing_sub,"ChangPing",weights,seed,sc)
//    val FangShan_sub = subset(FangShan)
//    splitWrite(FangShan_sub,"FangShan",weights,seed,sc)
//    val PingGu_sub = subset(PingGu)
//    splitWrite(PingGu_sub,"PingGu",weights,seed,sc)
//    val MenTouGou_sub = subset(MenTouGou)
//    splitWrite(MenTouGou_sub,"MenTouGou",weights,seed,sc)
//    val ShunYi_sub = subset(ShunYi)
//    splitWrite(ShunYi_sub,"ShunYi",weights,seed,sc)
//    val DongCheng_sub = subset(DongCheng)
//    splitWrite(DongCheng_sub,"DongCheng",weights,seed,sc)
//    val XiCheng_sub = subset(XiCheng)
//    splitWrite(XiCheng_sub,"XiCheng",weights,seed,sc)
//    val HaiDian_sub = subset(HaiDian)
//    splitWrite(HaiDian_sub,"HaiDian",weights,seed,sc)
//    val ShiJingShan_sub = subset(ShiJingShan)
//    splitWrite(ShiJingShan_sub,"ShiJingShan",weights,seed,sc)
//    val YanQing_sub = subset(YanQing)
//    splitWrite(YanQing_sub,"YanQing",weights,seed,sc)
//    val TongZhou_sub = subset(TongZhou)
//    splitWrite(TongZhou_sub,"TongZhou",weights,seed,sc)
//    val DaXing_sub = subset(DaXing)
//    splitWrite(DaXing_sub,"DaXing",weights,seed,sc)
//    val MiYun_sub = subset(MiYun)
//    splitWrite(MiYun_sub,"MiYun",weights,seed,sc)
//    val ChaoYang_sub = subset(ChaoYang)
//    splitWrite(ChaoYang_sub,"ChaoYang",weights,seed,sc)
//    val FengTai_sub = subset(FengTai)
//    splitWrite(FengTai_sub,"FengTai",weights,seed,sc)
//    val HuaiRou_sub = subset(HuaiRou)
//    splitWrite(HuaiRou_sub,"HuaiRou",weights,seed,sc)
//  }
//}
//
//object tdid_imei{
//
//  // Make sure the tdid is in correct format
//  def format(tdid:String) = tdid.toLowerCase().replaceAll("[^0-9a-z]", "")
//
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().set("spark.driver.maxResultSize","2g")
//    val sc = new SparkContext(conf)
//    val sqlContext = new SQLContext(sc)
//
//    val inputs = args(0).split(",")
//    val outputs = args(1).split(",")
//
//    val fm = new SimpleDateFormat("yyyyMMdd")
//
//    val id = sqlContext.read.parquet("/datascience/etl2/id/active/all/imei/info/2018/01/27")
//      .select("tdid","id","dateArray").rdd.map{
//      line =>
//        val tdid = format(line.getAs[String]("tdid"))
//        val imei = line.getAs[String]("id")
//        val dateArray = line.getAs[Seq[String]]("dateArray").reverse.head
//        (tdid,imei,dateArray)
//    }
//
//    inputs zip outputs foreach{
//      case(input, output) =>
//        if(PathUtil.isExisted(sc,output)){
//          PathUtil.deleteExistedPath(sc, output)
//        }
//
//        val orig_tdid = sc.textFile(input).map{
//          line =>
//            val tdid = line.split("\t")(0)
//            format(tdid)
//        }.collect.toSet
//
//        id.filter{
//          case(tdid,imei,dateArray) => orig_tdid.contains(tdid)
//        }.map{
//          case(tdid,imei,date)=>
//            tdid -> (imei,date)
//        }.reduceByKey((x1,x2) =>
//          // Find and keep the latest imei code and its update time
//          if(fm.parse(x1._2).getTime > fm.parse(x2._2).getTime){
//            x1
//          } else {
//            x2
//          }
//        ).map{
//          case(tdid,(imei,date)) =>
//            tdid + "\t" + imei
//        }.repartition(50).saveAsTextFile(output)
//    }
//
////    val all = new UnionRDD(sc,outputs.map{
////      path => sc.textFile(path).map{
////        line => line.split("\t")(1)
////      }
////    }).distinct.saveAsTextFile("/datalab/user/kefu/ageData/out_batches/test/all")
//  }
//}
