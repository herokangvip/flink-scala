package com.hk.sourceTest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
  * Description: 
  *
  * @author heroking
  * @version 1.0.0
  */
object SourceFromKafka {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "localhost:9092")
    prop.setProperty("group.id", "test-group")
    prop.setProperty("key-deserializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.setProperty("value-deserializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.setProperty("auto.offset.reset", "latest")
    //flink checkpoint偏移量状态也会保存，FlinkKafkaConsumer封装了保存offset的功能
    val dataStream = env.addSource(new FlinkKafkaConsumer011[String]("test-topic", new SimpleStringSchema(), prop))

    dataStream.print().setParallelism(1)
    env.execute("SourceFromKafka")
  }

}
