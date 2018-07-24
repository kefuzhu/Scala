//package HadoopRun
//
///**
//  * Created by TEND on 12/26/2017.
//  */
//object importantCode {
//  def main(args: Array[String]): Unit = {
//
////    Read Sequence File
//    val r = sc.sequenceFile[Text,BytesWritable](args(0)).flatMap{
//      case (key,value) =>
//        val len = value.getLength
//        val index = value.getBytes.indexOf('\0'.toByte)
//        val headerBytes = value.getBytes.slice(0, index)
//        val bodyBytes = value.getBytes.slice(index + 1, len)
//        apply(headerBytes,bodyBytes)
//    }.coalesce(args(2).toInt,false).saveAsTextFile(args(1))
//  }
//}


//val rdd = new UnionRDD(sc,hours.map{
//  h =>
//    sc.sequenceFile[Text,BytesWritable](s"$input/$h/*/")
//}).filter(_._2.getLength>1).map{
//  case (key,bytes) =>
//    val bs = bytes.getBytes
//    // some records start with {{ , 0x7b = '{'
//    if( bs(1) == 0x7b ) {
//      new String(bytes.getBytes.slice(1,bytes.getLength))
//    }else{
//      new String(bytes.getBytes.slice(0,bytes.getLength))
//    }
//}.flatMap{
//  line =>
//    try{
//      List(jackson.parseJson(line))
//    }catch {
//      case e : Exception => Nil
//    }
//}

