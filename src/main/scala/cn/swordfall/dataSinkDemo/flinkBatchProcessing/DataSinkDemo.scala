package cn.swordfall.dataSinkDemo.flinkBatchProcessing

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
  * @Author: Yang JianQiu
  * @Date: 2019/3/5 11:42
  *
  *  flink在批处理中常见的sink
  *  1.基于本地集合的sink(collection-based-sink)
  *  2.基于文件的sink(File-based-sink)
  */
class DataSinkDemo {

  /**
    * 基于本地集合的sink(Collection-based-sink)
    */
  def dataSinkFromLocal(): Unit ={
    //1.定义环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    //2.定义数据 stu(age, name, height)
    val stu: DataSet[(Int, String, Double)] = env.fromElements(
      (19, "zhangsan", 178.8),
      (17, "lisi", 168.8),
      (18, "wangwu", 184.8),
      (21, "zhaolilu", 164.8)
    )
    //3.sink到标准输出
    stu.print()

    //3.sink到标准error输出
    stu.printToErr()

    //4.sink到本地Collection
    println(stu.collect())
  }

  /**
    * 基于文件的sink(File-based-sink)
    *
    * flink支持多种存储设备上的文件，包括本地文件，hdfs文件，alluxio文件等。
    * flink支持多种文件的存储格式，包括text文件，CSV文件等
    */
  def dataSinkFromFile01(): Unit ={
    //0注意：不论是本地还是hdfs。若Parallelism > 1 将把path当成目录名称，若Parallelism = 1将把path当成文件名
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1: DataSet[(Int, String)] = env.fromCollection(Map(1 -> "spark", 2 -> "flink"))
    //1.写入到本地，文本文档，NO_OVERWRITE模式下如果文件已经存在，则报错，OVERWRITE模式下如果文件已经存在，则覆盖
    ds1.setParallelism(1).writeAsText("file:///output/flink/datasink/test01.txt", WriteMode.OVERWRITE)
    env.execute()

    //2.写入到hdfs，文本文档，路径不存在则自动创建路径
    ds1.setParallelism(1).writeAsText("hdfs://output/flink/datasink/test01.txt", WriteMode.OVERWRITE)
    env.execute()

    //3.写入到hdfs，CSV文档
    //3.1 读取csv文件
    val inPath = "hdfs://input/flink/sales.csv"
    case class Sales(transactionId: String, customerId: Int, itemId: Int, amountPaid: Double)
    val ds2 = env.readCsvFile[Sales](
      filePath = inPath,
      lineDelimiter = "\n",
      fieldDelimiter = ",",
      lenient = false,
      ignoreFirstLine = true,
      includedFields = Array(0, 1, 2, 3),
      pojoFields = Array("transactionId", "customerId", "itemId", "amountPaid")
    )

    //3.2 将csv文档写入到hdfs
    val outPath = "hdfs:///output/flink/datasink/sales.csv"
    ds2.setParallelism(1).writeAsCsv(filePath = outPath, rowDelimiter = "\n", fieldDelimiter = "|", WriteMode.OVERWRITE)
    env.execute()
  }

  /**
    * 基于文件的sink(数据进行排序)
    * 可以使用sortPartition对数据进行排序后再sink到外部系统。
    */
  def dataSinkFromFile02(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    //stu(age, name, height)
    val stu: DataSet[(Int, String, Double)] = env.fromElements(
      (19, "zhangsan", 178.8),
      (17, "lisi", 168.8),
      (18, "wangwu", 184.8),
      (21, "zhaoliu", 164.8)
    )
    //1.以age从小到大升序排列(0->9)
    stu.sortPartition(0, Order.ASCENDING).print()
    //2.以name从大到小降序排列(z->a)
    stu.sortPartition(1, Order.DESCENDING).print()
    //3.以age升序，height降序排序
    stu.sortPartition(0, Order.ASCENDING).sortPartition(2, Order.DESCENDING).print()
    //4.所有字段升序排序
    stu.sortPartition("_", Order.ASCENDING).print()
    //5.以Student.name升序
    //5.1准备数据
    case class Student(name: String, age: Int)
    val ds1: DataSet[(Student, Double)] = env.fromElements(
      (Student("zhangsan", 18), 178.5),
      (Student("lisi", 19), 176.5),
      (Student("wangwu", 17), 168.5)
    )
    val ds2 = ds1.sortPartition("_1.age", Order.ASCENDING).setParallelism(1)
    //5.2写入到hdfs，文本文档
    val outPath1 = "hdfs:///output/flink/datasink/Student001.txt"
    ds2.writeAsText(filePath = outPath1, WriteMode.OVERWRITE)
    env.execute()
    //5.3写入到hdfs, csv文档
    val outPath2 = "hdfs:///output/flink/datasink/Student002.csv"
    ds2.writeAsCsv(filePath = outPath2, rowDelimiter = "\n", fieldDelimiter = "|||", WriteMode.OVERWRITE)
    env.execute()
  }
}
