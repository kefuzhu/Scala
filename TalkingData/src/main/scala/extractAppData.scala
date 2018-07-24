/////**
////  * Created by kefu on 9/5/2017.
////  */
////import org.apache.spark.sql.functions._
////import java.text.SimpleDateFormat
////import org.apache.spark.{SparkConf, SparkContext}
////
////
////object extractData{
////  def main(args:Array[String]): Unit ={
////    //Convert string time to date format
////    val  fmt:SimpleDateFormat = new java.text.SimpleDateFormat("yyyy/MM/dd")
////    //Start date and end date from standard input
////    val startDay = args(0)
////    val endDay = args(1)
////    //init the spark
////    val sc = new SparkContext(new SparkConf().setAppName("ExtractData"))
////    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
////
////    //Calculate the number of days between start and end
////    def dayDiff(start:String,end:String):Int = {
////      val dayms = 1000 * 3600 * 24.0
////      val st = fmt.parse(start).getTime
////      val endDate = fmt.parse(end).getTime
////      val days = (endDate - st) / dayms
////      days.toInt
////    }
////
////    //Delete file if exists
////    def deleteFile(path: String) : Unit = {
////      import org.apache.hadoop.conf.Configuration
////      import org.apache.hadoop.fs.{FileSystem, Path}
////      val hdfs : FileSystem = FileSystem.get(new Configuration)
////      val isExist = hdfs.exists(new Path(path))
////      if (isExist){
////        hdfs.delete(new Path(path), true)//true: delete files recursively
////      }
////
////    //Function to extractAppData and aggregate by day
////    def extractAppData(in:String,out:String):Unit = {
////      //Read in data as dataframe
////      val df = sqlContext.read.parquet(in)
////      //Subset the dataframe
////      val subDF = df.select("tdid","app.pkgName","receiveTime").withColumn("receiveTime",to_date(from_unixtime(df("receiveTime")/1000)))
////
////      //Aggregate the info by day
////      val aggDF = subDF.rdd.map(row => (row,1)).reduceByKey(_ + _).map{row =>
////        val tdid =row._1.getAs[String](0)
////        val appKey =row._1.getAs[String](1)
////        val receiveTime =row._1.getAs[java.sql.Date](2)
////        val count =row._2
////        (tdid,appKey,receiveTime,count)}.toDF("tdid","appKey","receiveTime","count")
////
////      //Wrtie the dataframe to filapth
////      aggDF.repartition(20).write.format("csv").save(out)
////    }
////    //Set the input file path
////    val input = "/datascience/etl2/standard/ta/2017/06/01/*/"
////    //Set the output file path
////    val output = "kefu/2017JuneApp.csv"
////
////    //Delete current file if it exists
////    deleteFile(output)
////    //Extract the App Data
////    extractAppData(input,output)
////  }
////}
////
////
////// tdid, receiveTime, app.appName, appList.open, appList.run, appList.install, location.time, location.provider,
////// networks.activeNet, user.appStoreAccount, user.phoneNumber
////
//
