package com.sailabs

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object wordcount {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("wordcount")
    val sc = new SparkContext(sparkConf)

    val fileRDD = sc.textFile("/user/gobisubramani/testData/wordcount.txt", 4)

    val dataFlatMapRDD = fileRDD.flatMap(x => x.split(" "))

    val dataMapRDD = dataFlatMapRDD.map(x => (x, 1))

    // dataMapRDD.checkpoint()

    val dataReduceByKey = dataMapRDD.reduceByKey((x, y) => x + y)

    dataReduceByKey.saveAsTextFile("/user/gobisubramani/wordcountResult")

  }

}