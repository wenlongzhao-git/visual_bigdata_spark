package com.bigdata.test.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

object SparkDemo01 {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("SparkDemo01"))

    val a = sc.textFile("/test.txt")
    val b = a.map(line => line.length()).reduce(_ + _)


    println("ddddd:"+b)
  }
}
