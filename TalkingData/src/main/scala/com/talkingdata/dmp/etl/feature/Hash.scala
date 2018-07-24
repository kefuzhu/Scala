package com.talkingdata.dmp.etl.feature
import java.math.BigInteger
import java.util.StringTokenizer
import java.lang.{Float => Fl}

/**
  * Created by chris on 6/29/17.
  */
object Hash {
  def  Fnv1Hash(hashbits: Int,input: String): BigInteger = {
    val hashBits: Int = hashbits match {
      case 32 => hashbits
      case 64 => hashbits
      case 128 => hashbits
      case 256 => hashbits
      case 512 => hashbits
      case 1024 => hashbits
      case _ => 64
    }

    val MASK = hashbits match {
      case 32 => BigInteger.ONE.shiftLeft(hashbits).subtract(BigInteger.ONE)
      case 64 => BigInteger.ONE.shiftLeft(hashbits).subtract(BigInteger.ONE)
      case 128 => BigInteger.ONE.shiftLeft(hashbits).subtract(BigInteger.ONE)
      case 256 => BigInteger.ONE.shiftLeft(hashbits).subtract(BigInteger.ONE)
      case 512 => BigInteger.ONE.shiftLeft(hashbits).subtract(BigInteger.ONE)
      case 1024 => BigInteger.ONE.shiftLeft(hashbits).subtract(BigInteger.ONE)
      case _ => BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE)

    }
    var FNV_INIT = hashbits match {
      case 32 => new BigInteger(utils.FNV_32_INIT)
      case 64 => new BigInteger(utils.FNV_64_INIT)
      case 128 => new BigInteger(utils.FNV_128_INIT)
      case 256 => new BigInteger(utils.FNV_256_INIT)
      case 512 => new BigInteger(utils.FNV_512_INIT)
      case 1024 => new BigInteger(utils.FNV_1024_INIT)
      case _ => new BigInteger(utils.FNV_64_INIT)
    }
    var FNV_PRIME = hashbits match {
      case 32 => new BigInteger(utils.FNV_32_PRIME)
      case 64 => new BigInteger(utils.FNV_64_PRIME)
      case 128 => new BigInteger(utils.FNV_128_PRIME)
      case 256 => new BigInteger(utils.FNV_256_PRIME)
      case 512 => new BigInteger(utils.FNV_512_PRIME)
      case 1024 => new BigInteger(utils.FNV_1024_PRIME)
      case _ => new BigInteger(utils.FNV_64_PRIME)
    }
    var i = 0
    while (i < input.length) {
      FNV_INIT = FNV_INIT.multiply(FNV_PRIME)
      FNV_INIT = FNV_INIT.xor(BigInteger.valueOf(input.charAt(i)))
      i += 1
    }
    FNV_INIT.and(MASK)
  }

  def  Fnv1aHash(hashbits: Int,input: String): BigInteger = {
    val hashBits: Int = hashbits match {
      case 32 => hashbits
      case 64 => hashbits
      case 128 => hashbits
      case 256 => hashbits
      case 512 => hashbits
      case 1024 => hashbits
      case _ => 64
    }

    val MASK = hashbits match {
      case 32 => BigInteger.ONE.shiftLeft(hashbits).subtract(BigInteger.ONE)
      case 64 => BigInteger.ONE.shiftLeft(hashbits).subtract(BigInteger.ONE)
      case 128 => BigInteger.ONE.shiftLeft(hashbits).subtract(BigInteger.ONE)
      case 256 => BigInteger.ONE.shiftLeft(hashbits).subtract(BigInteger.ONE)
      case 512 => BigInteger.ONE.shiftLeft(hashbits).subtract(BigInteger.ONE)
      case 1024 => BigInteger.ONE.shiftLeft(hashbits).subtract(BigInteger.ONE)
      case _ => BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE)

    }
    var FNV_INIT = hashbits match {
      case 32 => new BigInteger(utils.FNV_32_INIT)
      case 64 => new BigInteger(utils.FNV_64_INIT)
      case 128 => new BigInteger(utils.FNV_128_INIT)
      case 256 => new BigInteger(utils.FNV_256_INIT)
      case 512 => new BigInteger(utils.FNV_512_INIT)
      case 1024 => new BigInteger(utils.FNV_1024_INIT)
      case _ => new BigInteger(utils.FNV_64_INIT)
    }
    var FNV_PRIME = hashbits match {
      case 32 => new BigInteger(utils.FNV_32_PRIME)
      case 64 => new BigInteger(utils.FNV_64_PRIME)
      case 128 => new BigInteger(utils.FNV_128_PRIME)
      case 256 => new BigInteger(utils.FNV_256_PRIME)
      case 512 => new BigInteger(utils.FNV_512_PRIME)
      case 1024 => new BigInteger(utils.FNV_1024_PRIME)
      case _ => new BigInteger(utils.FNV_64_PRIME)
    }
    var i = 0
    while (i < input.length) {
      FNV_INIT = FNV_INIT.xor(BigInteger.valueOf(input.charAt(i)))
      FNV_INIT = FNV_INIT.multiply(FNV_PRIME)
      i += 1
    }
    FNV_INIT.and(MASK)
  }

  def SimHash(tokens:Array[(String,Float)], hashBits:Int, f:(Int, String) => BigInteger): String = {
    val v = new Array[Float](hashBits)
    for (each <- tokens) {
      var weight = 1f
      try{
        weight = each._2
      } catch {
        case e: Exception =>
      }
      val t = f(hashBits,each._1)
      var i = 0
      while (i < hashBits) {
        val bitmask = new BigInteger("1").shiftLeft(i)
        if (t.and(bitmask).signum != 0) {
          v(i) += weight
        } else {
          v(i) -= weight
        }
        i += 1
      }
    }
    var fingerprint = new BigInteger("0")
    val simHashBuffer = new StringBuffer
    var i = 0
    while (i < hashBits) {
      if (v(i) >= 0) {
        fingerprint = fingerprint.add(new BigInteger("1").shiftLeft(i))
        simHashBuffer.append("1")
      } else {
        simHashBuffer.append("0")
      }
      i += 1
    }
    simHashBuffer.toString
  }

  def main(args: Array[String]): Unit = {
    val a = Array(("ai", 1.1f),("d",2f),("s",3f),("s",4f))
    val z = SimHash(a,64,Fnv1Hash)
    println(z)
  }
}
