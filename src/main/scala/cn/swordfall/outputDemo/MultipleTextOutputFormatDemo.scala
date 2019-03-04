package cn.swordfall.outputDemo

import java.util.Date

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.hadoop.mapred.HadoopOutputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.{FileOutputFormat, JobConf}
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

/**
  * @Author: Yang JianQiu
  * @Date: 2019/3/4 11:09
  */
class MultipleTextOutputFormatDemo[K, V] extends MultipleTextOutputFormat[K, V]{

  /**
    * 此方法用于产生文件名称，这里将key_DateTime直接作为文件名称
    * @param key
    * @param value
    * @param name
    * @return
    */
  //override def generateFileNameForKeyValue(key: K, value: V, name: String): String = key.asInstanceOf[String]
  override def generateFileNameForKeyValue(key: K, value: V, name: String): String = (key + "_" + new Date().getTime).asInstanceOf[String]
  //这里将name_key直接作为文件名称
  //override def generateFileNameForKeyValue(key: K, value: V, name: String): String = (name + "_" + key).asInstanceOf[String]

  /**
    * 此方法用于产生文件内容中的key，这里文件内容中的key是就是DataSet的key
    * @param key
    * @param value
    * @return
    */
  //override def generateActualKey(key: K, value: V): K = NullWritable.get().asInstanceOf[K]
  override def generateActualKey(key: K, value: V): K = key.asInstanceOf[K]

  /**
    * 此方法用于产生文件内容中的value，这里文件内容中的value就是DataSet的value
    * @param key
    * @param value
    * @return
    */
  override def generateActualValue(key: K, value: V): V = value.asInstanceOf[V]
}

/**
  * hadoop fs -text /output/flink/MultipleTextOutputFormat/scala/001/lisi
  */
object MultipleTextOutputFormatDemo{
  def main(args: Array[String]): Unit = {

    //1.创建批处理环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    //2.准备数据
    val data1 = env.fromCollection(List(("zhangsan", "120"), ("lisi", "123"), ("zhangsan", "309"), ("lisi", "207"), ("wangwu", "315")))
    data1.setParallelism(4)

    //3.多路径输出的HadoopOutputFormat
    val multipleTextOutputFormat = new MultipleTextOutputFormatDemo[String, String]()
    val jobConf = new JobConf()
    val filePath = "hdfs://192.168.187.201:9000/output/flink/multipleTextOutputFormat/scala/001"
    FileOutputFormat.setOutputPath(jobConf, new Path(filePath))
    val format = new HadoopOutputFormat[String, String](multipleTextOutputFormat, jobConf)

    //4.将数据输出出去
    data1.output(format)

    //5.触发批处理执行
    env.execute()
  }
}
