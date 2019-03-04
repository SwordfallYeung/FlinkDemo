package cn.swordfall.faultTolerantDemo

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.restartstrategy.RestartStrategies.FixedDelayRestartStrategyConfiguration
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

/**
  * @Author: Yang JianQiu
  * @Date: 2019/3/4 17:08
  *
  *  flink支持容错设置，当操作失败了，可以在指定重试的启动时间和重试的次数，有两种设置方式：
  *  1.通过配置文件，进行全局的默认设定
  *  2.通过程序的api进行设定
  *
  *
  *  1.通过配置flink-conf.yaml来设定全局容错
  *  设定出错重试3次
  *  execution-retries.default: 3
  *
  *  设定重试间隔时间5秒
  *  execution-retries.delay: 5s
  *
  *  2.程序的api进行容错设定
  *  flink支持通过api设定容错信息
  *  //失败重试3次
  *  env.setNumberOfExecutionRetries(3)
  *  //重试时延 5000 milliseconds
  *  env.getConfig.setExecutionRetryDelay(5000)
  */
class FaultTolerant {

  def faultTolerant: Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment

    //将来的版本中会丢弃
    //失败重试3次
    //env.setNumberOfExecutionRetries(3)
    //重试时延5000 milliseconds
    //env.getConfig.setExecutionRetryDelay(5000)

    //失败重试3次 重试时延5000 milliseconds 替换为
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(5, TimeUnit.SECONDS)))
    val ds1 = env.fromElements(2, 5, 3, 7, 9)
    ds1.map(_ * 2).print()
  }

}

object FaultTolerant{
  def main(args: Array[String]): Unit = {
    val f = new FaultTolerant
    f.faultTolerant
  }
}
