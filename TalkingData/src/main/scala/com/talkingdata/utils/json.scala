package com.talkingdata.utils
import org.json4s.JsonAST.{JDouble, JInt, JString}
import org.json4s._
/**
  * Created by takun on 2017/6/12.
  */
object json {

  def string(value:JValue) = value match {
    case JString(v) => v
    case _ => null
  }

  def int(value:JValue) = value match {
    case JInt(v) => v.toInt
    case _ => 0
  }

  def long(value:JValue) = value match {
    case JInt(v) => v.toLong
    case _ => 0L
  }

  def double(value:JValue) = value match {
    case JDouble(v) => v
    case _ => 0.0
  }

  def boolean(value:JValue) = value match {
    case JBool(v) => v
    case _ => false
  }

  def array(value:JValue) = value match {
    case JArray(arr) => arr
    case _ => List.empty[JValue]
  }

  def obj(value:JValue) = value match {
    case JObject(fs) => fs
    case _ => List.empty[(String,JValue)]
  }
}