package cn.swordfall.broadcaseVariablesAndDistributedCache

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.configuration.Configuration
import org.apache.flink.api.scala._

/**
  * @Author: Yang JianQiu
  * @Date: 2019/3/4 17:48
  *
  *  flink支持将变量广播到worker上，以供程序运算使用
  */
class BroadcastVariables extends Serializable {

  def broadcastVariables: Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    //1.准备工人数据（用于map）
    case class Worker(name: String, salaryPerMonth: Double)
    val workers: DataSet[Worker] = env.fromElements(
      Worker("zhangsan", 1356.67),
      Worker("lisi", 1476.67)
    )

    //2.准备统计数据(用于withBroadcastSet进行广播)
    case class Count(name: String, month: Int)
    val counts: DataSet[Count] = env.fromElements(
      Count("zhangsan", 4),
      Count("lisi", 5)
    )

    //3.使用map数据和广播数据进行计算
    workers.map(new RichMapFunction[Worker, Worker] {
      private var cwork: java.util.List[Count] = null

      override def open(parameters: Configuration): Unit = {
        //3.1访问广播数据
        cwork = getRuntimeContext.getBroadcastVariable[Count]("countWorkInfo")
      }

      override def map(value: Worker): Worker = {
        //3.2解析广播数据
        var i = 0
        while (i < cwork.size()) {
          val c = cwork.get(i)
          i += 1
          if (c.name.equalsIgnoreCase(value.name)) {
            //有相应的信息的返回值
            return Worker(value.name, value.salaryPerMonth * c.month)
          }
        }
        //吾响应的信息的返回值
        Worker("####", 0)
      }
    }).withBroadcastSet(counts, "countWorkInfo").print()
  }
}
object BroadcastVariables{
  def main(args: Array[String]): Unit = {
    val b = new BroadcastVariables
    b.broadcastVariables
  }
}
