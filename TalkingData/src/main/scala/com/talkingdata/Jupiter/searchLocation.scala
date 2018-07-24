package com.talkingdata.Jupiter

import com.talkingdata.datascience.tools.Convert
import com.talkingdata.dmp.algorithm.Location
import com.talkingdata.dmp.util.GeoHash
import gnu.trove.procedure.TIntProcedure
import net.sf.jsi.rtree.RTree
import net.sf.jsi.{Point, Rectangle}
import org.apache.spark.SparkContext

import scala.collection.mutable
import scala.io.Source

// Created by Jason.Ye
// Description:
//    Function    --  Geo Fence and R-Tree to locate Position
//    Create Time --  2016/10/09
//    Finish Time --  2016/11/04
// Copyright (c) 2016-present, TalkingData
object searchLocation {
  def getSegment(min:Double,max:Double,distinct:Double):Array[Double]={
    val array = (1 to ((max - min) / 0.05).toInt).map(i=>min + i * 0.05)
    array.toArray
  }
  /*
  * 方法名: 获取最大GeoHash前缀
  * 描述:  用于优化城市查询速度
  * */
  def getMaxSamePre(latlngs:Array[(Double,Double)])= {
    val abGeohash = for ((lat, lng) <- latlngs) yield GeoHash.encode(lat, lng, 7)
    var maxSamePre = ""
    var i = 7
    while (i > 0 && maxSamePre.length == 0) {
      if (abGeohash.map(_.substring(0, i)).distinct.length == 1) {
        maxSamePre = abGeohash(0).substring(0, i)
      }
      i -= 1
    }
    if (maxSamePre.length > 0) maxSamePre
    else
      "BIG"
  }
  def getCodeGeoArray(path:String,sc:SparkContext):Map[String, Set[(String, Array[(Double, Double)])]]={
    val geoSet = mutable.HashMap[String,Set[(String,Array[(Double,Double)])]]()
    sc.textFile(path).collect().foreach{row=>
      val Array(code,_,poly) = row.split("\t")
      val polyArray = poly.split(";").map(p=>(p.split(",")(0).toDouble,p.split(",")(1).toDouble))
      val geo = getMaxSamePre(polyArray)
      val set = geoSet.getOrElse(geo,Set[(String,Array[(Double,Double)])]()) ++ Set((code,polyArray))
      geoSet += geo -> set
    }
    geoSet.toMap
  }
  def getLocalCodeGeoArray(path:String):Map[String, Set[(String, Array[(Double, Double)])]]={
    val geoSet = mutable.HashMap[String,Set[(String,Array[(Double,Double)])]]()
    Source.fromFile(path).getLines().foreach{row=>
      val Array(code,_,poly) = row.split("\t")
      val polyArray = poly.split(";").map(p=>(p.split(",")(0).toDouble,p.split(",")(1).toDouble))
      val geo = getMaxSamePre(polyArray)
      val set = geoSet.getOrElse(geo,Set[(String,Array[(Double,Double)])]()) ++ Set((code,polyArray))
      geoSet += geo -> set
    }
    geoSet.toMap
  }
  def getCodeName(path:String,sc:SparkContext):Map[String,String]={
    sc.textFile(path).map(row=>row.split("\t")(0)->row.split("\t")(1)).collect.toMap
  }
  def getProvince(code:String,codeName:Map[String,String]):String={
    if(code!="null") {
      val provinceCode = code.take(2) + "0000"
      codeName(provinceCode)
    }
    else "null"
  }
  def getDistrict(code:String,codeName:Map[String,String]):String = {
    if(code!="null") {
      codeName(code)
    }
    else "null"
  }
  def getCity(code:String,codeName:Map[String,String]):String = {
    if(code!="null") {
      val cityCode = code.take(4) + "00"
      codeName(cityCode)
    }
    else "null"
  }
  def getCode(point:(Double,Double),cityPoly:Map[String, Set[(String, Array[(Double, Double)])]]):String = {
    val geoHash = GeoHash.encode(point._1,point._2,8)
    var areaName = "null"
    var i = 7
    while(areaName == "null"  && i >=0) {
      val key = if(i>0)geoHash.substring(0,i) else "BIG"
      cityPoly.get(key) match {
        case Some(cityPoints) =>
          for (s <- cityPoints if point._1 >= s._2.minBy(_._1)._1 &&
            point._1 <= s._2.maxBy(_._1)._1 &&
            point._2 >= s._2.minBy(_._2)._2 &&
            point._2 <= s._2.maxBy(_._2)._2 &&
            areaName == "null"
          ) {
            if(Convert.isPolyContainsPt(s._2,point))
              areaName = s._1
          }
        case  None =>
      }
      i -= 1
    }
    areaName
  }
  def getNationGeoEnity(path:String,sc:SparkContext)={
    val rtree = new RTree()
    rtree.init(null)
    var index = 0
    println("init")
    val indexArea = sc.textFile(path).collect().map {row =>
      var latMin = 90d
      var latMax = -90d
      var lngMin = 180d
      var lngMax = -180d
      val Array(_, nation, poly) = row.split("\t")
      val polyArray = poly.split(";").map { p =>
        val (lat, lng) = (p.split(",")(0).toDouble, p.split(",")(1).toDouble)
        if (lat > latMax) latMax = lat
        if (lat < latMin) latMin = lat
        if (lng > lngMax) lngMax = lng
        if (lng < lngMin) lngMin = lng
        (lat,lng)
      }
      rtree.add(new Rectangle(lngMin.toFloat, latMin.toFloat, lngMax.toFloat, latMax.toFloat), index)
      index += 1
      (index - 1) -> (nation, polyArray)
    }
    (rtree,indexArea.toMap)
  }
  def getCodeGeoEnity(path:String,sc:SparkContext)={
    val rtree = new RTree()
    rtree.init(null)
    var index = 0
    println("init")
    val indexArea = sc.textFile(path).collect().map {row =>
      var latMin = 90d
      var latMax = -90d
      var lngMin = 180d
      var lngMax = -180d
      val Array(code, _, poly) = row.split("\t")
      val polyArray = poly.split(";").map { p =>
        val (lat, lng) = (p.split(",")(0).toDouble, p.split(",")(1).toDouble)
        if (lat > latMax) latMax = lat
        if (lat < latMin) latMin = lat
        if (lng > lngMax) lngMax = lng
        if (lng < lngMin) lngMin = lng
        (lat,lng)
      }
      rtree.add(new Rectangle(lngMin.toFloat, latMin.toFloat, lngMax.toFloat, latMax.toFloat), index)
      index += 1
      (index - 1) -> (code, polyArray)
    }
    (rtree,indexArea.toMap)
  }
  def getLocalEnity(path:String)={
    val rtree = new RTree()
    rtree.init(null)
    var index = 0
    println("init")
    val indexArea = Source.fromFile(path).getLines().map { row =>
      var latMin = 90d
      var latMax = -90d
      var lngMin = 180d
      var lngMax = -180d
      val Array(code, _, poly) = row.split("\t")
      val polyArray = poly.split(";").map { p =>
        val (lat, lng) = (p.split(",")(0).toDouble, p.split(",")(1).toDouble)
        if (lat > latMax) latMax = lat
        if (lat < latMin) latMin = lat
        if (lng > lngMax) lngMax = lng
        if (lng < lngMin) lngMin = lng
        (lat,lng)
      }
      rtree.add(new Rectangle(lngMin.toFloat, latMin.toFloat, lngMax.toFloat, latMax.toFloat), index)
      index += 1
      (index - 1) -> (code, polyArray)
    }
    (rtree,indexArea.toMap)
  }
  def rtreeGetCode(point:(Double,Double),rtree:RTree,areaMap: Map[Int, (String, Array[(Double, Double)])])={
    val codeProc = new AreaProcedure(areaMap)
    rtree.nearest(new Point(point._2.toFloat,point._1.toFloat),codeProc,0l)
    val codeResult = codeProc.getAreaList.toArray
    val codeNum = codeResult.length
    val code = if(codeNum <= 0) "null" else if (codeNum == 1) codeResult.head._1 else{
      var preCode = "null"
      for(i <- codeResult if preCode == "null"){
        if(Location.isPolyContainsPt(i._2,(point._1,point._2)))
          preCode = i._1
      }
      preCode
    }
    code
  }

  def main(args: Array[String]) {
    val point = (39.1922790291,116.1601153242)
    var current = System.currentTimeMillis()
    val (codeRTree,codeAreaMap) = getLocalEnity("/Users/yejiesheng/IdeaProjects/location/Solar-System/codeDistrictPoly2016-03-29.txt")
    var loadTime = (System.currentTimeMillis() - current)
    println("RTree 加载数据时间" + loadTime + "ms")
    val codeProc = new AreaProcedure(codeAreaMap)
    codeRTree.nearest(new Point(point._2.toFloat,point._1.toFloat),codeProc,0l)
    val codeResult = codeProc.getAreaList.toArray
    val codeNum = codeResult.length
    val code = if(codeNum <= 0) "null" else if (codeNum == 1) codeResult.head._1 else{
      var preCode = "null"
      for(i <- codeResult if preCode == "null"){
        if(Location.isPolyContainsPt(i._2,(point._1,point._2)))
          preCode = i._1
      }
      preCode
    }
    println(code)
    var time = System.currentTimeMillis() - current
    println("RTree 总共费时:" + time + "ms")
    println("RTree 查询费时:" + (time - loadTime) + "ms")
    current = System.currentTimeMillis()
    val codeMap = getLocalCodeGeoArray("/Users/yejiesheng/IdeaProjects/location/Solar-System/codeDistrictPoly2016-03-29.txt")
    loadTime = System.currentTimeMillis() - current
    println("GeoFence 加载数据时间" + loadTime + "ms")
    val code2 = getCode(point,codeMap)
    println(code2)
    time = System.currentTimeMillis() - current
    println("GeoFence 执行时间" + time + "ms")
    println("GeoFenc 查询费时:" + (time - loadTime) + "ms")
  }
}

class AreaProcedure(areaMap:Map[Int, (String, Array[(Double, Double)])]) extends TIntProcedure with Serializable {
  val areaList = collection.mutable.ArrayBuffer[(String, Array[(Double, Double)])]()

  def execute(id: Int): Boolean = {
    val area = areaMap.getOrElse(id, ("null", Array((0d, 0d))))
    if (area != null) {
      areaList += area
    }
    true
  }

  def getAreaList: collection.mutable.ArrayBuffer[(String, Array[(Double, Double)])] = {
    areaList
  }
}
