package com.hk.sinkTest

import com.hk.transformTest.Sensor
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

/**
  * Description: kafka Sink输出
  *
  * @author heroking
  * @version 1.0.0
  */
object KafkaSinkTest {
  def main(args: Array[String]) {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //flatMap可以完成map和filter的操作，map和filter有明确的语义，转换和过滤更加直白
    val dataFromFile: DataStream[String] = env.readTextFile("E:\\workspace\\flink-scala\\src\\main\\resources\\sensors.txt")
    val dataStream = dataFromFile.map(data => {
      val array = data.split(",")
      Sensor(array(0).trim, array(1).trim.toLong, array(2).trim.toDouble).toString
    })

    dataStream.addSink(
      new FlinkKafkaProducer011[String]("localhost:9092", "test-topic", new SimpleStringSchema())
    )


    env.execute("kafka test sink")


  }
}
