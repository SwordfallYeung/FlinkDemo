package cn.swordfall.parameterTransformDemo

import java.io.Serializable

import org.apache.flink.api.common.functions.{MapFunction, RichFunction, RichMapFunction}
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

/**
  * @Author: Yang JianQiu
  * @Date: 2019/3/4 14:49
  *
  * 内部类需要序列化
  *
  * flink中的参数传递
  * flink中支持向Function传递参数，常见的有两种方式，
  * 1.通过构造方法向Function传递参数
  * 2.通过Execution向Function传递参数
  */
class ParameterTransform extends Serializable {

  /**
    * 1. 通过构造方法向Function传递参数(基本数据)
    */
  def parameterTransform01: Unit ={
    //1.创建批处理执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    //2.准备工资数据
    case class Worker(name: String, salaryPerMonth: Double)
    val salary = env.fromElements(2123.5, 5987.3, 7991.2)

    //2.准备补助数据
    val bouns = 38.111

    //3.计算工资和补助之和
    /*salary.map(value => {
      val total = value + bouns
      total
    }).print()*/
    salary.map(new SalarySumMap(bouns)).print()
    class SalarySumMap(b: Double) extends MapFunction[Double, Double] {
      override def map(s: Double): Double = {
        //工资 + 补助
        s + b
      }
    }
  }

  /**
    * 1. 通过构造方法向Function传递参数(复合数据)
    */
  def parameterTransform02: Unit ={
    //1.创建批处理执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    //2.准备工人数据
    case class Worker(name: String, salaryPerMonth: Double)
    val workers: DataSet[Worker] = env.fromElements(
      Worker("zhangsan", 1356.67),
      Worker("lisi", 1476.67)
    )

    //2.准备工作月份数据，作为参数传递出去
    val month = 4

    //3.接受参数进行计算
    workers.map(new SalarySumMap(month)).print()
    class SalarySumMap(m: Int) extends MapFunction[Worker, Worker]{
      override def map(w: Worker): Worker = {
        //取出参数，取出worker进行计算
        Worker(w.name, w.salaryPerMonth * m)
      }
    }
  }

  /**
    * 通过ExecutionConfig向Function传递参数
    */
  def parameterTransform03: Unit ={
    //1.创建批处理执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    //2.准备工人数据
    case class Worker(name: String, salaryPerMonth: Double)
    val workers: DataSet[Worker] =env.fromElements(
      Worker("zhangsan", 1356.67),
      Worker("lisi", 1476.67)
    )

    //2.准备工作月份数据，作为参数用configuration传递出去
    val conf = new Configuration()
    conf.setString("month", "4")
    env.getConfig.setGlobalJobParameters(conf)

    //3.接受参数进行计算(如果要用Configuration传参，需要用RichMapFunction接收)
    workers.map(new RichMapFunction[Worker, Worker]{
      private var m = 0

      override def open(configuration: Configuration): Unit = {
        //3.1 获取Configuration传递过来的参数
        val globalParams = this.getRuntimeContext.getExecutionConfig.getGlobalJobParameters
        m = globalParams.toMap.get("month").trim.toInt
      }

      override def map(in: Worker): Worker = {
        //3.2 计算最新工人工资信息
        Worker(in.name, in.salaryPerMonth * m)
      }
    }).print()
  }
}
object ParameterTransform{
  def main(args: Array[String]): Unit = {
    val p = new ParameterTransform
    //p.parameterTransform01
    //p.parameterTransform02
    p.parameterTransform03
  }
}
