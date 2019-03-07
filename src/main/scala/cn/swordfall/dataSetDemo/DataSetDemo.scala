package cn.swordfall.dataSetDemo

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.util.Collector

/**
  * @Author: Yang JianQiu
  * @Date: 2019/3/5 16:37
  *
  */
object DataSetDemo {
  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    //创建一个DataSet其元素为String类型
    val input1: DataSet[String] = env.fromElements("A", "B", "C")

    //1.print 将息输出到标准输出设备  将DataSet的内容打印出来
    input1.print()

    //2.printToErr 将信息输出到标准错误输出 将DataSet的内容打印出来
    input1.printToErr()

    //3.count 计算DataSet中元素的个数
    input1.count()

    //4.min 获取最小的元素
    case class Student1(age: Int, name: String, height: Double)
    val input2: DataSet[Student1] = env.fromElements(
      Student1(16, "zhangsan", 194.5),
      Student1(17, "zhangsan", 184.5),
      Student1(18, "zhangsan", 174.5),
      Student1(16, "lisi", 194.5),
      Student1(17, "lisi", 184.5),
      Student1(18, "lisi", 174.5)
    )
    //4.1获取age最小的元素
    input2.min(0).collect()

    //4.2获取age最小的元素
    input2.min("age").collect()

    //5.max 获取最大的元素
    //5.1获取age最大的元素
    input2.max(0).collect()

    //5.2获取age最大的元素
    input2.max("age").collect()


    //6.sum 获取元素的的累加和，只能作用于数值类型
    //6.1 fieldIndex=0的列进行sum
    input2.sum(0).collect()

    //6.2 fieldName="age"的列进行sum
    input2.sum("age").collect()

    //6.3 fieldName="height"的列进行sum
    input2.sum("height").collect()

    //7.getType 获取DataSet的元素的类型信息
    input1.getType()

    //8.map 将一个DataSet转换成另一个DataSet。转换操作对每一个元素执行一次。
    val input3: DataSet[Int] = env.fromElements(23, 67, 18, 29, 32, 56, 4, 27)
    //将DataSet中的每个元素乘以2
    input3.map(_ * 2).collect()

    //9.flatMap
    val input4: DataSet[String] = env.fromElements("zhangsan boy", "lisi girl")
    //将DataSet中的每个元素用空格切割成一组单词
    input4.flatMap(_.split(" ")).collect()


    //10.mapPartition 和map类似，不同它的处理单位是partition，而非element。
    val input5: DataSet[String] = env.fromElements("zhangsan boy", "lisi is a girl so sex")
    //获取partition的个数
    input5.mapPartition(in => Some(in.size)).collect()

    //11.filter 过滤满足添加的元素，不满足条件的元素将被丢弃！
    val input6: DataSet[String] = env.fromElements("zhangsan boy", "lisi is a girl so sex","wangwu boy")
    input6.filter(_.contains("boy")).collect()

    //12.reduce 根据一定的条件和方式来合并DataSet。
    val a1: DataSet[Int] = env.fromElements(2, 5, 9, 8, 7, 3)
    a1.reduce(_ + _).collect()

    val a2: DataSet[String] = env.fromElements("zhangsan boy", "lisi girl")
     a2.reduce(_ + _).collect()


    //13.groupBy 暗示第二个输入较小的交叉。拿第一个输入的每一个元素和第二个输入的每一个元素进行交叉操作。
    case class WC(val word: String, val salary: Int)
    val words: DataSet[WC] = env.fromElements(
      WC("LISI", 600), WC("LISI", 400), WC("WANGWU", 300), WC("ZHAOLIU", 700)
    )
    //13.1.1使用一个case class Fields
    // 使用key-expressions
    words.groupBy("word").reduce(
      (w1, w2) => new WC(w1.word, w1.salary + w2.salary)
    ).collect()

    //13.1.2使用key-selector
    words.groupBy(_.word).reduce(
      (w1, w2) => new WC(w1.word, w1.salary + w2.salary)
    ).collect()

    //13.2.1使用多个case class Fields
    case class Student2(val name: String, addr: String, salary: Double)
    val tuples: DataSet[Student2] = env.fromElements(
      Student2("lisi","shandong",2400.00),Student2("zhangsan","henan",2600.00),
      Student2("lisi","shandong",2700.00),Student2("lisi","guangdong",2800.00)
    )
    //13.2.2使用自定义的reduce方法，使用多个Case Class Fields name
    tuples.groupBy("name", "addr").reduce(
      (s1, s2) => Student2(s1.name + "-" + s2.name, s1.addr + "-" + s2.addr, s1.salary + s2.salary)
    ).collect()

    //13.2.3使用自定义的reduce方法，使用多个Case Class Fields index
    tuples.groupBy(0, 1).reduce(
      (s1, s2) => Student2(s1.name + "-" + s2.name, s1.addr + "-" + s2.addr, s1.salary + s2.salary)
    ).collect()

    //14.ReduceGroup 此函数和reduce函数类似，不过它每次处理一个grop而非一个元素。
    val input7: DataSet[(Int, String)] = env.fromElements(
      (20, "zhangsan"),(22, "zhangsan"),
      (22, "lisi"), (20, "zhangsan")
    )
    //14.1.1先用String分组，然后对分组进行reduceGroup
    input7.groupBy(1).reduceGroup{
      //将相同的元素用set去重
      (in, out: Collector[(Int, String)]) =>
        in.toSet foreach(out.collect)
    }.collect()

    //14.2.1
    case class Student3(age: Int, name: String)
    //14.2.2 创建DataSet[Student]
    val input8: DataSet[Student3] = env.fromElements(
      Student3(20, "zhangsan"),
      Student3(22, "zhangsan"),
      Student3(22, "lisi"),
      Student3(20, "zhangsan")
    )
    //14.2.3以age进行分组，然后对分组进行reduceGroup
    input8.groupBy(_.age).reduceGroup(
      //将相同的元素用set去重
      (in, out: Collector[Student3]) =>
        in.toSet foreach(out.collect)
    ).collect()

    //15.sortGroup 对分组好的排序
    val input9: DataSet[(Int, String)] = env.fromElements(
      (20,"zhangsan"),
      (22,"zhangsan"),
      (22,"lisi"),
      (22,"lisi"),
      (22,"lisi"),
      (18,"zhangsan"),
      (18,"zhangsan"))
    //15.1用int分组，用int对分组进行排序
    val sortData = input9.groupBy(0).sortGroup(0, Order.ASCENDING)
    //15.2对排序好的分组进行reduceGroup
    sortData.reduceGroup(
      //将相同的元素用set去重
      (in, out: Collector[(Int, String)]) =>
        in.toSet foreach(out.collect)
    ).collect()

    //16. minBy 在分组后的数据中，获取每组最小的元素
    case class Student4(age: Int, name: String, height:Double)
    //16.1创建DataSet[Student]
    val input10: DataSet[Student4] = env.fromElements(
      Student4(16,"zhangasn",194.5),
      Student4(17,"zhangasn",184.5),
      Student4(18,"zhangasn",174.5),
      Student4(16,"lisi",194.5),
      Student4(17,"lisi",184.5),
      Student4(18,"lisi",174.5))
    //16.2 以name进行分组，获取age最小的元素
    input10.groupBy(_.name).minBy(0).collect()

    //16.3 以name进行分组，获取height和age最小的元素
    input10.groupBy(_.name).minBy(2, 0).collect()


    //17. maxBy 在分组后的数据中，获取每组最大的元素
    //17.1 以name进行分组，获取age最大的元素
    input10.groupBy(_.name).maxBy(0).collect()

    //17.2 以name进行分组，获取height和age最大的元素
    input10.groupBy(_.name).maxBy(2, 0).collect()


    //18.distinct 对DataSet中的元素进行去重
    val input11: DataSet[String] = env.fromElements("lisi","zhangsan", "lisi","wangwu")
    //18.1单一项目元素去重
    input11.distinct().collect()

    //18.2多项目去重，不指定比较项目，默认是全部比较
    val input12: DataSet[(Int, String, Double)] =  env.fromElements(
      (2,"zhagnsan",1654.5),(3,"lisi",2347.8),(2,"zhagnsan",1654.5),
      (4,"wangwu",1478.9),(5,"zhaoliu",987.3),(2,"zhagnsan",1654.0))
    input12.distinct().collect()

    //18.3多项目的去重，指定比较项目, 元素去重：指定比较第0和第1号元素
    input12.distinct(0, 1).collect()

    //18.4case class的去重，指定比较项目
    case class Student5(name : String, age : Int)
    val input13: DataSet[Student5] = env.fromElements(
      Student5("zhangsan", 24),Student5("zhangsan",24),Student5("zhangsan",25),
      Student5("lisi",24),Student5("wangwu",24),Student5("lisi",25)
    )
    //去掉age重复的元素
    val age_r = input13.distinct("age")
    age_r.collect()
    //去掉age重复的元素
    val name_r = input13.distinct("name")
    name_r.collect()
    //去掉name和age重复的元素
    val all_r = input13.distinct("age","name")
    all_r.collect
    //去掉name和age重复的元素
    val all = input13.distinct()
    all.collect
    //去掉name和age重复的元素
    val all0 = input13.distinct("_")
    all0.collect
    //18.5 根据表达式进行去重
    val input14: DataSet[Int] = env.fromElements(3,-3,4,-4,6,-5,7)
    //根据表达式，本例中是根据元素的绝对值进行元素去重
    input14.distinct {x => Math.abs(x)}.collect


    //19.join 将两个DataSet进行join操作
    //19.1创建一个 DataSet其元素为[(Int,String)]类型
    val input15: DataSet[(Int, String)] = env.fromElements(
      (2,"zhagnsan"),(3,"lisi"),(4,"wangwu"),(5,"zhaoliu"))
    //创建一个 DataSet其元素为[(Double, Int)]类型
    val input16: DataSet[(Double, Int)] = env.fromElements(
      (1850.98,4),(1950.98,5),(2350.98,6),(3850.98,3))
    //两个DataSet进行join操作，条件是input1(0)==input2(1)
    input15.join(input16).where(0).equalTo(1).collect()

    //19.2
    case class Rating(name: String, category: String, points: Int)
    //定义DataSet[Rating]
    val ratings: DataSet[Rating] = env.fromElements(
      Rating("moon","youny1",3),Rating("sun","youny2",4),
      Rating("cat","youny3",1),Rating("dog","youny4",5))
    //创建DataSet[(String, Double)]
    val weights: DataSet[(String, Double)] = env.fromElements(
      ("youny1",4.3),("youny2",7.2),
      ("youny3",9.0),("youny4",1.5))
    //使用方法进行join
    ratings.join(weights).where("category").equalTo(0) {
      (rating, weight) => (rating.name, rating.points + weight._2)
    }.collect
    //19.3
    ratings.join(weights).where("category").equalTo(0){
      (rating, weight, out: Collector[(String, Double)]) =>
        if (weight._2 > 0.1) out.collect(rating.name, rating.points * weight._2)
    }.collect()

    //19.4 执行join操作时暗示数据大小
    val input17: DataSet[(Int, String)] =
      env.fromElements((3,"zhangsan"),(2,"lisi"),(4,"wangwu"),(6,"zhaoliu"))
    //定义 DataSet[(Int, String)]
    val input18: DataSet[(Int, String)] =
      env.fromElements((4000,"zhangsan"),(70000,"lisi"),(4600,"wangwu"),(53000,"zhaoliu"))
    //暗示第二个输入很小
    input17.joinWithTiny(input18).where(1).equalTo(1).collect

    // 4.暗示第二个输入很大
    input17.joinWithHuge(input18).where(1).equalTo(1).collect

    //19.5 flink有很多种执行join的策略，你可以指定一个执行策略，以便提高执行效率。
    val input19: DataSet[(Int, String)] =
      env.fromElements((3,"zhangsan"),(2,"lisi"),(4,"wangwu"),(6,"zhaoliu"))
    val input20: DataSet[(Int, String)] =
      env.fromElements((4000,"zhangsan"),(70000,"lisi"),(4600,"wangwu"),(53000,"zhaoliu"))
    //暗示input2很小
    input19.join(input20, JoinHint.BROADCAST_HASH_FIRST).where(1).equalTo(1).collect

    /*暗示有如下选项：
    1.JoinHint.OPTIMIZER_CHOOSES:
      没有明确暗示，让系统自行选择。
    2.JoinHint.BROADCAST_HASH_FIRST
      把第一个输入转化成一个哈希表，并广播出去。适用于第一个输入数据较小的情况。
    3.JoinHint.BROADCAST_HASH_SECOND:
      把第二个输入转化成一个哈希表，并广播出去。适用于第二个输入数据较小的情况。
    4.JoinHint.REPARTITION_HASH_FIRST:（defalut）
      1.如果输入没有分区，系统将把输入重分区。
      2.系统将把第一个输入转化成一个哈希表广播出去。
      3.两个输入依然比较大。
      4.适用于第一个输入小于第二个输入的情况。
    5.JoinHint.REPARTITION_HASH_SECOND:
      1.如果输入没有分区，系统将把输入重分区。
      2.系统将把第二个输入转化成一个哈希表广播出去。
      3.两个输入依然比较大。
      4.适用于第二个输入小于第一个输入的情况。
    6.JoinHint.REPARTITION_SORT_MERGE:
      1.如果输入没有分区，系统将把输入重分区。
      2.如果输入没有排序，系统将吧输入重排序。
      3.系统将合并两个排序好的输入。
      4.适用于一个或两个分区已经排序好的情况。*/

    //20.leftOuterJoin 左外连接
    val ratings2: DataSet[Rating] = env.fromElements(
      Rating("moon","youny1",3),Rating("sun","youny2",4),
      Rating("cat","youny3",1),Rating("dog","youny4",5),Rating("tiger","youny4",5))
    val movies: DataSet[(String, String)] = env.fromElements(
      ("moon", "ok"), ("dog", "good"), ("cat", "notbad"), ("sun", "nice"),("water", "nice")
    )
    //20.1 两个dataset进行左外连接，指定方法
    movies.leftOuterJoin(ratings2).where(0).equalTo("name"){
      (m, r) => (m._1, if (r == null) -1 else r.points)
    }.collect()

    //20.2 两个dataset进行左外连接，指定连接暗示，并指定连接方法
    movies.leftOuterJoin(ratings2, JoinHint.REPARTITION_SORT_MERGE).where(0).equalTo("name"){
      (m, r) => (m._1, if (r == null) -1 else r.points)
    }.collect()

    /*左外连接支持以下项目：
    JoinHint.OPTIMIZER_CHOOSES
    JoinHint.BROADCAST_HASH_SECOND
    JoinHint.REPARTITION_HASH_SECOND
    JoinHint.REPARTITION_SORT_MERGE*/

    //21.rightOuterJoin 右外连接
    //21.1 两个dataset进行左外连接，指定连接方法
    movies.rightOuterJoin(ratings2).where(0).equalTo("name"){
      (m, r) => (if (m == null) -1 else m._1, if (r == null) -1 else r.points)
    }.collect()

    //21.2 两个dataset进行右外连接，指定连接暗示，并指定连接方法
    movies.rightOuterJoin(ratings2, JoinHint.BROADCAST_HASH_FIRST).where(0).equalTo("name"){
      (m, r) => (if (m == null) -1 else m._1, if (r == null) -1 else r.points)
    }.collect()
    /*右外连接支持以下项目：
    JoinHint.OPTIMIZER_CHOOSES
    JoinHint.BROADCAST_HASH_FIRST
    JoinHint.REPARTITION_HASH_FIRST
    JoinHint.REPARTITION_SORT_MERGE*/

    //22. fullOuterJoin 全外连接
    //22.1 两个dataset进行全外连接，指定连接方法
    movies.fullOuterJoin(ratings2).where(0).equalTo("name"){
      (m, r) => (m._1, if (r == null) -1 else r.points)
    }.collect()

    //22.2 两个dataset进行全外连接，指定连接暗示，并指定连接方法
    movies.fullOuterJoin(ratings2, JoinHint.REPARTITION_SORT_MERGE).where(0).equalTo("name"){
      (m, r) => (m._1, if (r == null) -1 else r.points)
    }.collect()

    //23. cross 交叉。拿第一个输入的每一个元素和第二个输入的每一个元素进行交叉操作。
    //23.1 基本tuple
    val coords1 = env.fromElements((1,4,7),(2,5,8),(3,6,9))
    val coords2 = env.fromElements((10,40,70),(20,50,80),(30,60,90))
    coords1.cross(coords2).collect()

    //23.2 case class
    case class Coord(id: Int, x: Int, y: Int)
    val coords3: DataSet[Coord] = env.fromElements(Coord(1, 4, 7), Coord(2, 5, 8), Coord(3, 6, 9))
    val coords4: DataSet[Coord] = env.fromElements(Coord(10, 40, 70), Coord(20, 50, 80), Coord(30, 60, 90))
    //交叉两个DataSet[Coord]
    coords3.cross(coords4).collect()

    //23.3 自定义操作
    //交叉两个DataSet[Coord]，使用自定义方法
    val result18 = coords3.cross(coords4){
      (c1, c2) => {
        val dist = (c1.x + c2.x) + (c1.y + c2.y)
        (c1.id, c2.id, dist)
      }
    }.collect()

    //24.crossWithTiny 暗示第二个输入较小的交叉。拿第一个输入的每一个元素和第二个输入的每一个元素进行交叉操作。
    coords3.crossWithTiny(coords4).collect()

    //25.crossWithHuge 暗示第二个输入较大的交叉。拿第一个输入的每一个元素和第二个输入的每一个元素进行交叉操作。
    coords3.crossWithHuge(coords4).collect()


  }
}
