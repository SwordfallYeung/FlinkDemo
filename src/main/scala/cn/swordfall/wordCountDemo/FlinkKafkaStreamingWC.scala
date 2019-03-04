package cn.swordfall.wordCountDemo

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.log4j.{Level, Logger}
import org.apache.flink.streaming.api.scala._

/**
  * @Author: Yang JianQiu
  * @Date: 2019/3/2 16:32
  */
object FlinkKafkaStreamingWC {

  def main(args: Array[String]): Unit = {
    //1.关闭日志，可以减少不必要的日志输出
    Logger.getLogger("org").setLevel(Level.OFF)

    //2.指定kafka数据流的相关信息
    val kafkaCluster = "192.168.187.201:9092"
    val topic = "test"

    //3.创建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //4.创建kafka数据流
    val props = new Properties()
    props.put("bootstrap.servers", "192.168.187.201:9092")
    props.put("group.id", "kv_flink")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val myConsumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema, props)
    val text = env.addSource(myConsumer)

    //5.执行计算
    val counts = text.flatMap(_.toLowerCase()
      .split("\\W+"))
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    counts.print()

    //6.触发运算
    env.execute("flink-kafka-wordcount")
  }

}
