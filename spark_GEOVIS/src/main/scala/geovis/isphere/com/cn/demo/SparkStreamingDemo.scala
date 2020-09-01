package geovis.isphere.com.cn.demo

import org.apache.spark._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream

object SparkStreamingDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkDemo")

    val sc: SparkContext = new SparkContext(sparkConf)

    val ssc: StreamingContext = new StreamingContext(sc,Seconds(5))

    val dataStream: ReceiverInputDStream[String] = ssc.socketTextStream("master", 8888)

    dataStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
