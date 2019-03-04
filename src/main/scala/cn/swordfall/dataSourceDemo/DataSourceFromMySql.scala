package cn.swordfall.dataSourceDemo

import java.sql.{Connection, DriverManager, PreparedStatement}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

/**
  * @Author: Yang JianQiu
  * @Date: 2019/3/3 16:20
  */
class DataSourceFromMySql extends RichSourceFunction[Student]{
  private var connection: Connection = null
  private var ps: PreparedStatement = null

  /**
    * 一、open()方法中建立连接，这样不用每次invoke的时候都要建立连接和释放连接。
    * @param parameters
    */
  override def open(parameters: Configuration): Unit = {
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://192.168.187.201:3306/test"
    val username = "root"
    val password = "admin"

    //1.加载驱动
    Class.forName(driver)
    //2.创建连接
    connection = DriverManager.getConnection(url, username, password)
    //3.获得执行语句
    val sql = "select stuId, stuName, stuAddr, stuSex from Student;"
    ps = connection.prepareStatement(sql)
  }

  /**
    *  二、DataStream调用一次run()方法用来获取数据
    * @param sourceContext
    */
  override def run(sourceContext: SourceFunction.SourceContext[Student]): Unit = {
    val resultSet = ps.executeQuery()
    try {
      while (resultSet.next()) {
        val student = Student(
          resultSet.getInt("stuId"),
          resultSet.getString("stuName").trim,
          resultSet.getString("stuAddr").trim,
          resultSet.getString("stuSex").trim
        )
        sourceContext.collect(student)
      }
    } catch {
      case e: Exception => println(e.getMessage)
    }
  }

  override def cancel(): Unit = {

  }

  /**
    *  三、 程序执行完毕就可以进行，关闭连接和释放资源的动作了
    */
  override def close(): Unit = {
    //5.关闭连接和释放资源
    if (connection != null) {
      connection.close()
    }
    if (ps != null) {
      ps.close()
    }
  }
}

/**
  *        测试数据：
  * 1.查询数据库
  *        SHOW DATABASES;
  *
  * 2.创建数据库
  *        CREATE DATABASE flinktest;
  *
  * 3.使用数据库
  *        USE flinktest;
  *
  * 4.创建表格
  *        CREATE TABLE Student
  *        (
  *        stuid INT(11) PRIMARY KEY NOT NULL AUTO_INCREMENT,
  *        stuname VARCHAR(10) NOT NULL,
  *        stuaddr VARCHAR(40) NOT NULL,
  *        stusex VARCHAR(10) NOT NULL
  *        );
  *
  * 5.插入数据
  *        INSERT INTO Student(stuid,stuname,stuaddr,stusex)VALUES(1,"xiaoming","henan zhengzhou", "male")
  *        INSERT INTO Student(stuid,stuname,stuaddr,stusex)VALUES(2,"xiaoqiang","shandong jinan", "female")
  *        INSERT INTO Student(stuid,stuname,stuaddr,stusex)VALUES(3,"xiaohua","hebei shijiazhuang",   "male")
  *        INSERT INTO Student(stuid,stuname,stuaddr,stusex)VALUES(4,"xiaohong","yunnan kunming",  "female")
  *
  * 6.查询数据
  *        SELECT * FROM Student ;
  */
object DataSourceFromMySql{
    def main(args: Array[String]): Unit = {
      //1.创建流执行环境
      val env = StreamExecutionEnvironment.getExecutionEnvironment

      //2.从自定义source中读取数据
      val dataStream: DataStream[Student] = env.addSource(new DataSourceFromMySql)

      //3.显示结果
      dataStream.print()

      //4.触发流执行
      env.execute()
    }
}

case class Student(stuId: Int, stuName: String, stuAddr: String, stuSex: String)