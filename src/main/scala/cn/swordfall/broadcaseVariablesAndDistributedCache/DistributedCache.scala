package cn.swordfall.broadcaseVariablesAndDistributedCache

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.configuration.Configuration

import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * @Author: Yang JianQiu
  * @Date: 2019/3/4 20:24
  *
  * hdfs:///input/flink/workcount.txt文件内容如下：
  * zhagnsan:4
  * lisi:5
  */
class DistributedCache {

  def distributedCache: Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment

    //1.准备缓存数据
    val path = "hdfs://input/flink/workcount.txt"
    env.registerCachedFile(path, "MyTestFile")

    //2.准备工人数据
    case class Worker(name: String, salaryPerMonth: Double)
    val workers: DataSet[Worker] = env.fromElements(
      Worker("zhangsan", 1356.67),
      Worker("lisi", 1476.67)
    )

    //3.使用缓存数据和工人数据做计算
    workers.map(new MyMapper())
    class MyMapper() extends RichMapFunction[Worker, Worker]{
      private var lines: ListBuffer[String] = new ListBuffer[String]

      //3.1在open方法中获取缓存文件
      override def open(parameters: Configuration): Unit = {
        //通过RuntimeContext和DistributedCache接收缓存文件
        val myFile = getRuntimeContext.getDistributedCache.getFile("MyTestFile")
        val lines = Source.fromFile(myFile.getAbsoluteFile).getLines()
        lines.foreach(f = line => {
          this.lines.append(line)
        })
      }

      //3.2 在map方法中使用获取到的缓存文件内容
      override def map(value: Worker): Worker = {
        var name = ""
        var month = 0
        //分解文件中的内容
        for (s <- this.lines){
          val tokens = s.split(";")
          if (tokens.length == 2){
            name = tokens(0).trim
            if (name.equalsIgnoreCase(value.name)){
                month = tokens(1).trim.toInt
            }
          }
          //找到满足条件的信息
          if (name.nonEmpty && month > 0.0){
            return Worker(value.name, value.salaryPerMonth * month)
          }
        }
        //没有满足条件的信息
        Worker(value.name, value.salaryPerMonth * month)
      }
    }
  }

}
