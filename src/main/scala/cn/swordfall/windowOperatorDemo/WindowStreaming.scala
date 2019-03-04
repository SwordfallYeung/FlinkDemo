package cn.swordfall.windowOperatorDemo

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, GlobalWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._

/**
  * @Author: Yang JianQiu
  * @Date: 2019/3/2 21:40
  *
  *  1.源源不断的数据流是无法进行统计工作的，因为数据流没有边界，就无法统计到底有多少数据经过这个流。
  *     也无法统计数据流中的最大值，最小值，平均值，累加值等信息。
  *  2.如果在数据流上，截取固定大小的一部分，这部分是可以进行统计的。截取方式主要有两种，
  *     ①根据时间进行截取(time-driven-window)，比如每1分钟统计一次或者每10分钟统计一次。
  *     ②根据数据进行截取(data-driven-window)，比如每5个数据统计一次或者每50个数据统计一次。
  *
  *   1.flink支持两种划分窗口的方式（time和count）
  *      如果根据时间划分窗口，那么它就是一个time-window
  *      如果根据数据划分窗口，那么它就是一个count-window
  *   2.flink支持窗口的两个重要属性（size和interval）
  *      如果size=interval，那么就会形成tumbling-window（无重叠数据）
  *      如果size > interval，那么就会形成sliding-window（有重叠数据）
  *      如果size < interval，那么这种窗口将会丢失数据。比如每5秒钟，统计过去3秒的通过路口汽车的数据，将会漏掉2秒钟的数据。
  *   3.通过组合可以得出四种基本窗口
  *      time-tumbling-window 无重叠数据的时间窗口，设置方式举例：timeWindow(Time.seconds(5))
  *      time-sliding-window  有重叠数据的时间窗口，设置方式举例：timeWindow(Time.seconds(5), Time.seconds(3))
  *      count-tumbling-window无重叠数据的数量窗口，设置方式举例：countWindow(5)
  *      count-sliding-window 有重叠数据的数量窗口，设置方式举例：countWindow(5,3)
  *    4.flink支持在stream上的通过key去区分多个窗口
  *
  *    time-window的高级用法
  *    1.现实世界中的时间是不一致的，在flink中被划分为事件时间，提取时间，处理时间三种、
  *    2.如果以EventTime为基准来定义时间窗口那将形成EventTimeWindow,要求消息本身就应该携带EventTime
  *    3.如果以IngesingtTime为基准来定义时间窗口那将形成IngestingTimeWindow,以source的systemTime为准。
  *    4.如果以ProcessingTime基准来定义时间窗口那将形成ProcessingTimeWindow，以operator的systemTime为准。
  *
  */
object WindowStreaming {

  def main(args: Array[String]): Unit = {

  }

  /**
    * 交通场景下time-window实战
    *
    * 每5秒钟统计一次，在这过去的5秒钟内，各个路口通过红绿灯汽车的数量。
    *
    * 1.发送命令
    * nc -lk 9999
    *
    * 2.发送内容
    * 9,3
    * 9,2
    * 9,7
    * 4,9
    * 2,6
    * 1,5
    * 2,3
    * 5,7
    * 5,4
    *
    */

  /**
    * 翻滚窗口(Tumbling Windows)
    *
    * 翻滚窗口概念定义
    * // tumbling event-time windows
    *   input
    *        .keyBy(<key selector>)
    *        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
    *        .<windowed transformation>(<window function>)
    *
    * // tumbling processing-time windows
    *    input
    *         .keyBy(<key selector>)
    *         .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
    *         .<windowed transformation>(<window function>)
    * tumbling-time-window(无重叠数据)实战
    */
  def tumblingTimeWindow(): Unit ={
    //1.创建运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //2.定义数据流来源
    val text = env.socketTextStream("192.168.187.201", 9999)

    //3.转换数据格式，text -> CarWc
    case class CarWc(sensorId: Int, carCnt: Int)
    val ds1: DataStream[CarWc] = text.map((f) => {
        val tokens = f.split(",")
        CarWc(tokens(0).trim.toInt, tokens(1).trim.toInt)
      })

    //4.执行统计操作，每个sensorId一个tumbling窗口，窗口的大小为5秒
    //也就是说，每5秒钟统计一次，在这过去的5秒钟内，各个路口通过红绿灯汽车的数量
    val ds2: DataStream[CarWc] = ds1.keyBy("sensorId")
      //.window(TumblingEventTimeWindows.of(Time.seconds(5)))同下一句操作
      .timeWindow(Time.seconds(5))
      .sum("carCnt")

    //5.显示统计结果
    ds2.print()

    //6.触发流计算
    env.execute(this.getClass.getName)
  }

  /**
    * 滑动窗口（Sliding Windows）
    *  // sliding event-time windows
    *    input
    *         .keyBy(<key selector>)
    *         .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
    *         .<windowed transformation>(<window function>)
    *
    * // sliding processing-time windows
    *     input
    *          .keyBy(<key selector>)
    *          .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
    *          .<windowed transformation>(<window function>)
    * sliding-time-window(有重叠数据)实战
    */
  def slidingTimeWindow(): Unit ={
    //1.创建运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //2.定义数据流来源
    val text = env.socketTextStream("192.168.187.201", 9999)

    //3.转换数据格式，text -> CarWc
    case class CarWc(sensorId: Int, carCnt: Int)
    val ds1: DataStream[CarWc] = text.map((f) => {
      val tokens = f.split(",")
      CarWc(tokens(0).trim.toInt, tokens(1).trim.toInt)
    })

    //4.执行统计操作，每个sensorId一个tumbling窗口，窗口的大小为5秒
    //也就是说，每5秒钟统计一次，在这过去的5秒钟内，各个路口通过红绿灯汽车的数量
    val ds2: DataStream[CarWc] = ds1.keyBy("sensorId")
      .timeWindow(Time.seconds(10), Time.seconds(5))
      .sum("carCnt")

    //5.显示统计结果
    ds2.print()

    //6.触发流计算
    env.execute(this.getClass.getName)
  }

  /**
    * 还有两种窗口：全局窗口global windows和会话窗口session windows
    */

  /**
    * 全局窗口
    * 全局窗口定义
    * //global windows
    *   input
    *        .keyBy(<key selector>)
    *        .window(GlobalWindows.create())
    */
  def globalWindows: Unit ={
    //1.创建运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //2.定义数据流来源
    val text = env.socketTextStream("192.168.187.201", 9999)

    //3.转换数据格式，text -> CarWc
    case class CarWc(sensorId: Int, carCnt: Int)
    val ds1: DataStream[CarWc] = text.map((f) => {
      val tokens = f.split(",")
      CarWc(tokens(0).trim.toInt, tokens(1).trim.toInt)
    })

    //4.执行全局统计操作
    val ds2: DataStream[CarWc] = ds1.keyBy("sensorId")
      .window(GlobalWindows.create())
      .sum("carCnt")

    //5.显示统计结果
    ds2.print()

    //6.触发流计算
    env.execute(this.getClass.getName)
  }

  /**
    * 会话窗口 Session Windows
    * 会话窗口定义
    * // event-time session windows
    *  input
    *       .keyBy(<key selector>)
    *       .window(EventTimeSessionWindows.withGap(Time.minutes(10)))
    *       .<windowed transformation>(<window function>)
    *
    * // processing-time session windows
    *   input
    *        .keyBy(<key selector>)
    *        .window(ProcessingTimeSessionWindows.withGap(Time.minutes(10)))
    *        .<windowed transformation>(<window function>)
    */
  def sessionWindows: Unit ={
    //1.创建运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //2.定义数据流来源
    val text = env.socketTextStream("192.168.187.201", 9999)

    //3.转换数据格式，text -> CarWc
    case class CarWc(sensorId: Int, carCnt: Int)
    val ds1: DataStream[CarWc] = text.map((f) => {
      val tokens = f.split(",")
      CarWc(tokens(0).trim.toInt, tokens(1).trim.toInt)
    })

    //4.执行全局统计操作
    val ds2: DataStream[CarWc] = ds1.keyBy("sensorId")
      .window(EventTimeSessionWindows.withGap(Time.minutes(10)))
      .sum("carCnt")

    //5.显示统计结果
    ds2.print()

    //6.触发流计算
    env.execute(this.getClass.getName)
  }

  /**
    * 使用EventTime用法示例
    *
    * 要求消息本身就应该携带EventTime
    *
    * 以EventTime划分窗口，计算5秒钟内出价最高的信息
    *
    * 输入数据：
    * 1461756862000,boos1,pc1,100.0
    * 1461756867000,boos2,pc1,200.0
    * 1461756872000,boos1,pc1,300.0
    * 1461756862000,boos2,pc2,500.0
    * 1461756867000,boos2,pc2,600.0
    * 1461756872000,boos2,pc2,700.0
    */
  def eventTimeDemo(): Unit ={
    //1.创建执行环境，并设置为使用EventTime
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //2.创建数据流，并进行数据转化
    val source = env.socketTextStream("192.168.187.201", 9999)
    case class SalePrice(time: Long, boosName: String, produceName: String, price: Double)
    val dst1: DataStream[SalePrice] = source.map(value => {
      val columns = value.split(",")
      SalePrice(columns(0).toLong, columns(1), columns(2), columns(3).toDouble)
    })

    //3.使用EventTime进行求最值操作
    val dst2: DataStream[SalePrice] = dst1
    //提取消息中的时间戳属性
      .assignAscendingTimestamps(_.time)
      .keyBy(_.produceName)
    //.timeWindow(Time.seconds(5)) 设置window方法一
      .window(TumblingEventTimeWindows.of(Time.seconds(5))) //设置window方法二
      .max("price")

    //4.显示结果
    dst2.print()

    //5.触发流计算
    env.execute()
  }

  /**
    * 使用ProcessingTime用法示例
    *
    * 1.以operator的系统时间为准划分窗口，计算5秒钟内出价最高的信息
    * 2.因为是以实际的operator的systemTime为标准，那么消息中可以没有睡觉属性。
    * 3.flink默认的就是这种时间窗口
    *
    * 测试数据：
    * 1461756862000,boos1,pc1,100.0
    * 1461756867000,boos2,pc1,200.0
    * 1461756872000,boos1,pc1,300.0
    * 1461756862000,boos2,pc2,500.0
    * 1461756867000,boos2,pc2,600.0
    * 1461756872000,boos2,pc2,700.0
    */
  def processingTimeDemo: Unit ={

    //1.创建执行环境，并设置为使用ProcessingTime
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置为使用ProcessingTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    //2.创建数据流，并进行数据转化
    val source = env.socketTextStream("qingcheng11", 9999)
    case class SalePrice(time: Long, boosName: String, productName: String, price: Double)
    val dst1: DataStream[SalePrice] = source.map(value => {
      val columns = value.split(",")
      SalePrice(columns(0).toLong, columns(1), columns(2), columns(3).toDouble)
    })

    //3.使用ProcessingTime进行求最值操作,不需要提取消息中的时间属性
    val dst2: DataStream[SalePrice] = dst1
      .keyBy(_.productName)
      //.timeWindow(Time.seconds(5))//设置window方法一
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))//设置window方法二
      .max("price")

    //4.显示结果
    dst2.print()

    //5.触发流计算
    env.execute()
  }

}
