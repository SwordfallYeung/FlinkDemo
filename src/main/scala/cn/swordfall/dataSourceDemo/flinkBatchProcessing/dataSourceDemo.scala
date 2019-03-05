package cn.swordfall.dataSourceDemo.flinkBatchProcessing

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

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

  /**
    * 基于本地集合的source
    */
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

    //4.用List创建DataSet
    val ds4: DataSet[String] = env.fromCollection(List("spark", "flink"))
    ds4.print()

    //5.用ListBuffer创建DataSet
    val ds5: DataSet[String] = env.fromCollection(ListBuffer("spark", "flink"))
    ds5.print()

    //6.用Vector创建DataSet
    val ds6: DataSet[String] = env.fromCollection(Vector("spark", "flink"))
    ds6.print()

    //7.用Queue创建DataSet
    val ds7: DataSet[String] = env.fromCollection(mutable.Queue("spark", "flink"))
    ds7.print()

    //8.用Stack创建DataSet
    val ds8: DataSet[String] = env.fromCollection(mutable.Stack("spark", "flink"))
    ds8.print()

    //9.用Stream创建DataSet(Stream相当于lazy List，避免在中间过程中生成不必要的集合)
    val ds9: DataSet[String] = env.fromCollection(Stream("spark", "flink"))
    ds9.print()

    //10.用Seq创建DataSet
    val ds10: DataSet[String] = env.fromCollection(Seq("spark", "flink"))
    ds10.print()

    //11.用Set创建DataSet
    val ds11: DataSet[String] = env.fromCollection(Set("spark", "flink"))
    ds11.print()

    //12.用Iterable创建DataSet
    val ds12: DataSet[String] = env.fromCollection(Iterable("spark", "flink"))
    ds12.print()

    //13.用ArraySeq创建DataSet
    val ds13: DataSet[String] = env.fromCollection(mutable.ArraySeq("spark", "flink"))
    ds13.print()

    //14.用ArrayStack创建DataSet
    val ds14: DataSet[String] = env.fromCollection(mutable.ArrayStack("spark", "flink"))
    ds14.print()

    //15.用Map创建DataSet
    val ds15: DataSet[(Int, String)] = env.fromCollection(Map(1 -> "spark", 2 -> "flink"))

    //16.用Range创建创建DataSet
    val ds16: DataSet[Long] = env.fromCollection(Range(1, 9))
    ds16.print()

    //17.用fromElements创建DataSet
    val ds17: DataSet[Long] = env.generateSequence(1, 9)
    ds17.print()
  }

  /**
    * 基于文件的source （File-based-source）
    * flink支持多种存储设备上的文件，包括本地文件，hdfs文件，alluxio文件等。
    * flink支持多种文件的存储格式，包括text文件，CSV文件等。
    */
  def dataSourceFromFile01(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment

    //1.读取本地文本文件，本地文件以file://开头
    val ds1: DataSet[String] = env.readTextFile("file://test.txt")
    ds1.print()

    //2.读取hdfs文本文件，hdfs文件以hdfs://开头，不指定master的短URL
    val ds2: DataSet[String] = env.readTextFile("hdfs://input/flink/test.txt")
    ds2.print()

    //3.读取hdfs CSV文件，转化为tuple
    val path = "hdfs://192.168.187.201:9000/input/flink/sales.csv"
    val ds3 = env.readCsvFile[(String, Int, Int, Double)](
      filePath = path,
      lineDelimiter = "\n",
      fieldDelimiter = ",",
      lenient = false,
      ignoreFirstLine = true,
      includedFields = Array(0, 1, 2, 3)
    )
    ds3.print()

    //4.读取hdfs csv文件，转化为case class
    case class Sales(transactionId: String, customerId: Int, itemId: Int, amountPaid: Double)
    val ds4 = env.readCsvFile[Sales](
      filePath = path,
      lineDelimiter = "\n",
      fieldDelimiter = ",",
      lenient = false,
      ignoreFirstLine = true,
      includedFields = Array(0, 1, 2, 3),
      pojoFields = Array("transactionId", "customerId", "itemId", "amountPaid")
    )
    ds4.print()
  }

  /**
    * 基于文件的source（遍历目录）
    *
    * flink支持对一个文件目录内的所有文件，包括所有子目录中的所有文件的遍历访问方式。
    *
    * 递归读取hdfs目录中的所有文件，会遍历各级子目录
    */
  def dataSourceFromFile02(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment

    val parameters = new Configuration()
    //设置recursive enumeration parameter
    parameters.setBoolean("recursive.file.enumeration", true)
    //把配置传递到dataSource
    val ds1 = env.readTextFile("hdfs://input/flink").withParameters(parameters)
    ds1.print()
  }
}
