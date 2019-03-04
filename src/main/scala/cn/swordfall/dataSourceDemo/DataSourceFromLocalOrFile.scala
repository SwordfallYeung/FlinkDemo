package cn.swordfall.dataSourceDemo

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import org.apache.flink.streaming.api.scala._
/**
  * @Author: Yang JianQiu
  * @Date: 2019/3/3 21:48
  */
class DataSourceFromLocalOrFile {
  /**
    * 基本本地集合的source
    */
  def localSource(): Unit ={
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    //1.用element创建DataStream.fromElements()
    val ds1: DataStream[String] = senv.fromElements("spark", "flink")
    ds1.print()

    //2.用Tuple创建DataStream.fromElements()
    val ds2: DataStream[(Int, String)] = senv.fromElements((1, "spark"), (2, "flink"))
    ds2.print()

    //3.用Array创建DataStream
    val ds3: DataStream[String] = senv.fromCollection(ArrayBuffer("spark", "flink"))
    ds3.print()

    //4.用ArrayBuffer创建DataStream
    val ds4: DataStream[String] = senv.fromCollection(ArrayBuffer("spark", "flink"))
    ds4.print()

    //5.用List创建DataStream
    val ds5: DataStream[String] = senv.fromCollection(ListBuffer("spark", "flink"))
    ds5.print()

    //6.用Vector创建DataStream
    val ds6: DataStream[String] = senv.fromCollection(Vector("spark", "flink"))
    ds6.print()

    //7.用Queue创建DataStream
    val ds7: DataStream[String] = senv.fromCollection(mutable.Queue("spark", "flink"))
    ds7.print()

    //8.用Stack创建DataStream
    val ds8: DataStream[String] = senv.fromCollection(mutable.Stack("spark", "flink"))
    ds8.print()

    //9.用Stream创建DataStream(Stream相当于lazy List，避免在中间过程中生成不必要的集合)
    val ds9: DataStream[String] = senv.fromCollection(Stream("spark", "flink"))
    ds9.print()

    //10.用Seq创建DataStream
    val ds10: DataStream[String] = senv.fromCollection(Seq("spark", "flink"))
    ds10.print()

    //11.用Set创建DataStream(不支持)
    //val ds11: DataStream[String] = senv.fromCollection(Set("spark", "flink"))
    //ds11.print()

    //12.用Iterable创建DataStream(不支持)
    //val ds12: DataStream[String] = senv.fromCollection(Iterable("spark", "flink"))
    //ds12.print()

    //13.用ArraySeq创建DataStream
    val ds13: DataStream[String] = senv.fromCollection(mutable.ArraySeq("spark", "flink"))
    ds13.print()

    //14.用ArrayStack创建DataStream
    val ds14: DataStream[String] = senv.fromCollection(mutable.ArrayStack("spark", "flink"))
    ds14.print()

    //15.用Map创建DataStream(不支持)
    //val ds15: DataStream[(Int, String)] = senv.fromCollection(Map(1 -> "spark", 2 -> "flink"))
    //ds15.print()

    //16.用Range创建DataStream
    val ds16: DataStream[Int] = senv.fromCollection(Range(1, 9))
    ds16.print()

    //17.用fromElements创建DataStream
    val ds17: DataStream[Long] = senv.generateSequence(1, 9)
    ds17.print()

    senv.execute(this.getClass.getName)
  }

  /**
    * 基于文件的source
    */
  def fileSource(): Unit ={
    //1.创建运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //2.读取本地文件
    val text1 = env.readTextFile("file:///text.txt")
    text1.print()

    //2.读取hdfs文件
    val text2 = env.readTextFile("hdfs://192.168.187.201:9000/input/flink/text.txt")
    text2.print()

    env.execute()
  }
}

object DataSourceFromLocalOrFile{
  def main(args: Array[String]): Unit = {

  }
}
