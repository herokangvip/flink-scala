package com.hk.processFunctionTest

import java.util.Properties

import com.hk.transformTest.Sensor
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector


/**
  * Description: 使用ProcessFunction实现，流的切分功能，异常温度单独放到一个流里，和split类似
  *
  * @author heroking
  * @version 1.0.0
  */
object SideOutPutTest {
  def main(args: Array[String]) {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

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
      .process(new HotAlarm())


    dataStream.print("in:")
    //获取侧输出流信息
    val sideOutPut = result.getSideOutput(new OutputTag[String]("hot"))
    sideOutPut.print("侧输出流")

    result.print("out:")

    env.execute("TransformTest")
  }
}

//第二个参数是主输出流将要输出的数据类型
/**
  * 如果温度过高，输出报警信息到侧输出流
  */
class HotAlarm extends ProcessFunction[Sensor, Sensor] {

  //泛型为侧输出流要输出的数据类型
  lazy val alarmOutPutStream = new OutputTag[String]("hot")

  override def processElement(sensor: Sensor, context: ProcessFunction[Sensor, Sensor]#Context, collector: Collector[Sensor]): Unit = {
    if (sensor.temperature > 0.5) {
      context.output(alarmOutPutStream, "高温报警流")
    } else {
      collector.collect(sensor)
    }
  }
}
