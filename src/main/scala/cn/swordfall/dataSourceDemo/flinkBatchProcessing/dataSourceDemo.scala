package cn.swordfall.dataSourceDemo.flinkBatchProcessing

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}

import scala.collection.mutable.ArrayBuffer

/**
  * @Author: Yang JianQiu
  * @Date: 2019/3/5 1:02
  *
  *  flink在批处理中常见的source
  *  flink在批处理中常见的source主要有两类。
  *  1.基于本地集合的source(Collecction-based-source)
  *      在flink最常见的创建DataSet方式有三种：
  *      ①使用env.fromElements()，这种方式也支持Tuple，自定义对象等复合形式。
  *      ②使用env.fromCollection()，这种方式支持多种Collection的具体类型
  *      ③使用env.generateSequence()方法创建基于Sequence的DataSet
  *  2.基于文件的source(File-based-source)
  */
class dataSourceDemo {

  def dataSourceFromLocal(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    //0.用element创建DataSet(fromElements)
    val ds0: DataSet[String] = env.fromElements("spark", "flink")
    ds0.print()

    //1.用Tuple创建DataSet(fromElements)
    val ds1: DataSet[(Int, String)] = env.fromElements((1, "spark"), (2, "flink"))
    ds1.print()

    //2.用Array创建DataSet
    val ds2: DataSet[String] = env.fromCollection(Array("spark", "flink"))
    ds2.print()

    //3.用ArrayBuffer创建DataSet
    val ds3: DataSet[String] = env.fromCollection(ArrayBuffer("spark", "flink"))
    ds3.print()
  }
}
