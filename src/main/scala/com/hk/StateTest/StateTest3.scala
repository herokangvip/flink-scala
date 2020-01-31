package com.hk.StateTest

import java.util.Properties

import com.hk.transformTest.Sensor
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector

/**
  * Description: 温度差10度报警,使用flatMapWithState实现，ProcessFunction是底层api
  *
  * @author heroking
  * @version 1.0.0
  */
object StateTest3 {
  def main(args: Array[String]) {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //checkPoint默认是不开启的
    //env.enableCheckpointing(1000L)
    //env.setStateBackend(new MemoryStateBackend())

    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "localhost:9092")
    prop.setProperty("group.id", "test-group")
    prop.setProperty("key-deserializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.setProperty("value-deserializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.setProperty("auto.offset.reset", "latest")
    //flink checkpoint偏移量状态也会保存，FlinkKafkaConsumer封装了保存offset的功能
    val kafkaDataStream = env.addSource(new FlinkKafkaConsumer011[String]("test-topic", new SimpleStringSchema(), prop))


    val dataStream: DataStream[Sensor] = kafkaDataStream.map(data => {
      val array = data.split(",")
      new Sensor(array(0).trim, array(1).trim.toLong, array(2).trim.toDouble)
    })


    //窗口分类：固定时间窗口、滑动时间窗口、滚动计数窗口，滑动计数窗口，window/timeWindow/countWindow
    //window方法必须在keyBy后才能用，windowAll可以用在之前但一般不用
    //allowedLateness窗口延迟关闭，控制窗口的销毁
    val result = dataStream
      .keyBy(_.id)
      //R:输出类型，S：状态的类型
      .flatMapWithState[(String, Double, Double), Double] {
      //如果没有状态也就是没有数据来过，将当前数据存入状态
      case (input: Sensor, None) => (List.empty, Some(input.temperature))
      //如果有状态，和上次的状态值比较差值
      case (input: Sensor, lastTemp: Some[Double]) =>
        val diff = (input.temperature - lastTemp.get).abs
        if (diff > 10) {
          (List {
            (input.id, lastTemp.get, input.temperature)
          }, Some(input.temperature))
        } else {
          (List.empty, Some(input.temperature))
        }
    }


    result.print("out:")

    env.execute("TransformTest")
  }
}

/**
  * 构造函数参数，IO:in、out
  */
class MyFlatMap(param: Double) extends RichFlatMapFunction[Sensor, (String, Double, Double)] {

  var lastTemp: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    //初始化的时候声明state变量
    lastTemp = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  }


  //可以直接定义或者使用生命周期方法，open
  //lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))

  override def flatMap(sensor: Sensor, collector: Collector[(String, Double, Double)]): Unit = {
    if ((sensor.temperature - lastTemp.value()).abs > param) {
      collector.collect("sensor_" + sensor.id + ":温度超限", lastTemp.value(), sensor.temperature)
    }
    lastTemp.update(sensor.temperature)
  }

}
