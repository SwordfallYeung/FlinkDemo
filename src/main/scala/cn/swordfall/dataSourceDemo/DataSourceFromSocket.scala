package cn.swordfall.dataSourceDemo

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * @Author: Yang JianQiu
  * @Date: 2019/3/4 0:59
  */
class DataSourceFromSocket {

  /**
    * 基于网络套接字的source
    */
  def socketSource: Unit ={
    //1.创建运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //2.定义text1数据流，采用默认值，行分隔符为'\n', 失败重试0次
    val text1 = env.socketTextStream("192.168.187.201", 9999)
    text1.print()

    //2. 定义text2数据流，行分隔符为"|"，失败重试3次
    val text2 = env.socketTextStream("192.168.187.201", 9999, delimiter = '|', maxRetry = 3)
    text2.print()

    //3.触发计算
    env.execute(this.getClass.getName)
  }
}
object DataSourceFromSocket{
  def main(args: Array[String]): Unit = {

  }
}
