package geovis.isphere.com.cn.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("sparkdemo2")

    val sc: SparkContext = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("file/*")

    val value1 = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)


    val tuples2= value1.collect()

    tuples2.foreach(println)

    sc.stop()
  }
}
