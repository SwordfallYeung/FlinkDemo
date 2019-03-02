package cn.swordfall

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
//隐式转换，否则报错
import org.apache.flink.streaming.api.scala._
/**
  * @Author: Yang JianQiu
  * @Date: 2019/3/2 15:58
  *
  *  参考资料：
  *  https://www.iteblog.com/archives/2047.html
  */
object WordCountLocalStrings {

  def main(args: Array[String]): Unit = {

  }

  /**
    * 数据集合
    */
  def dataCollectionWordCount(): Unit ={
    //1.创建流处理环境
    val senv = StreamExecutionEnvironment.getExecutionEnvironment

    //2.准备数据
    val text = senv.fromElements(
      "To be, or not to be,that is the question",
      "Whether 'tis nobler in the mind to suffer",
      "The slings and arrows of outrageous fortune",
      "Or to take arms against a sea of troubles,")

    //3.执行计算
    val counts = text.flatMap(_.toLowerCase.split("\\W+"))
      .map((_, 1)).keyBy(0).sum(1)

    //4.将结果打印出来
    counts.print()

    //5.触发流计算
    senv.execute("Flink Streaming Wordcount")
  }

  /**
    * 本地文件或者hdfs上的文件
    */
  def fileWordCount(): Unit ={
    //1.创建流处理环境
    val senv = StreamExecutionEnvironment.getExecutionEnvironment

    //2.准备数据
    val text = senv.readTextFile("test.txt")

    //③socket数据
    val text3 = senv.socketTextStream("192.168.187.201", 9999)

    //3.执行计算
    val counts = text.flatMap(_.toLowerCase.split("\\W+"))
      .map((_, 1)).keyBy(0).sum(1)

    //4.将结果打印出来
    counts.print()

    //5.触发流计算
    senv.execute("Flink Streaming Wordcount")
  }

  /**
    * 网络socket
    */
  def SocketWindowWordCount(): Unit ={
    //1.创建流处理环境
    val senv = StreamExecutionEnvironment.getExecutionEnvironment

    //2.准备数据
    val text = senv.socketTextStream("192.168.187.201", 9999)

    //3.执行计算
    val counts = text.flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)

    //4.将结果打印出来
    counts.print()

    //5.触发流计算
    senv.execute("Window Streaming Wordcount")
  }
}
