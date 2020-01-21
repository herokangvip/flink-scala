package com.hk.sinkTest


import java.sql.{PreparedStatement, Connection, DriverManager}

import com.hk.transformTest.Sensor
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

/**
  * Description: 
  *
  * @author heroking
  * @version 1.0.0
  */
object JdbcSinkTest {
  def main(args: Array[String]) {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //flatMap可以完成map和filter的操作，map和filter有明确的语义，转换和过滤更加直白
    val dataFromFile: DataStream[String] = env.readTextFile("E:\\workspace\\flink-scala\\src\\main\\resources\\sensors.txt")
    val dataStream = dataFromFile.map(data => {
      val array = data.split(",")
      Sensor(array(0).trim, array(1).trim.toLong, array(2).trim.toDouble)
    })

    dataStream.addSink(new MyJdbcSink())


    env.execute("kafka test sink")


  }
}

class MyJdbcSink() extends RichSinkFunction[Sensor] {
  //定义sql链接，预编译器
  var conn: Connection = _
  //插入
  var insertStmt: PreparedStatement = _
  //更新
  var updateStmt: PreparedStatement = _

  //初始化
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/mfcq", "root", "root")
    insertStmt = conn.prepareStatement("insert into xx (name,temp) values (?,?)")
    updateStmt = conn.prepareStatement("update xx set temp=? where name=?")
  }

  override def invoke(value: Sensor, context: Context[_]): Unit = {
    //更新
    updateStmt.setDouble(1, value.temperature)
    updateStmt.setString(2, value.id)
    updateStmt.execute()
    //......

  }

  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}
