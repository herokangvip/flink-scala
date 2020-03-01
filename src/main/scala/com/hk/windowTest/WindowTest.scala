package com.hk.windowTest

import java.util.Properties

import com.hk.transformTest.Sensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.SimpleStringSchema


/**
  * Description: 统计5s内最低温度数据
  * watermark 主要是为了解决数据乱序到达的问题
  * allowed 解决窗口触发后数据迟到后的问题
  * https://blog.csdn.net/qq_22222499/article/details/94997611
  *
  * @author heroking
  * @version 1.0.0
  */
object WindowTest {
  def main(args: Array[String]) {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //给每一个stream追加eventTime时间特性
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //设置waterMark自动生成的时间间隔，不设置默认是200ms
    //env.getConfig.setAutoWatermarkInterval(500)

    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "localhost:9092")
    prop.setProperty("group.id", "test-group")
    prop.setProperty("key-deserializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.setProperty("value-deserializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.setProperty("auto.offset.reset", "latest")
    //flink checkpoint偏移量状态也会保存，FlinkKafkaConsumer封装了保存offset的功能
    val kafkaDataStream = env.addSource(new FlinkKafkaConsumer011[String]("test-topic", new SimpleStringSchema(), prop))


    //val dataFromFile: DataStream[String] = env.readTextFile("E:\\workspace\\flink-scala\\src\\main\\resources\\sensors.txt")

    val dataStream: DataStream[Sensor] = kafkaDataStream.map(data => {
      val array = data.split(",")
      new Sensor(array(0).trim, array(1).trim.toLong, array(2).trim.toDouble)
    })

    //对于确定已排好序的数据，非乱序时间设置水位线
    //dataStream.assignAscendingTimestamps(_.timestamp*1000)
    //处理乱序时间,eventTime取数据的timestamp，waterMark水位线延迟取1秒，例如时间戳5s水位线4s
    val waterMarkDataStream = dataStream.assignTimestampsAndWatermarks(
      //waterMark水位线延迟取1秒，将数据提前一秒，199数据到来认为是198之前的全来了
      //后面再触发窗口操作时也以eventTime(和waterMark有关)为准，waterMark控制窗口的激活时间
      new BoundedOutOfOrdernessTimestampExtractor[Sensor](Time.milliseconds(1000)) {
        override def extractTimestamp(t: Sensor): Long = t.timestamp * 1000
      }
    )

    //窗口分类：固定时间窗口、滑动时间窗口、滚动计数窗口，滑动计数窗口，window/timeWindow/countWindow
    //window方法必须在keyBy后才能用，windowAll可以用在之前但一般不用
    //allowedLateness窗口延迟关闭，控制窗口的销毁
    //sideOutputLateData,负责收集窗口销毁后的数据到新的侧输出流，flink结合waterMark、allowedLateness、sideOutputLateData保证数据准确性
    val outputTag = new OutputTag[(String, Double)]("side")
    val result = waterMarkDataStream
      .map(data => (data.id, data.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(2))
      .allowedLateness(Time.seconds(2))
      .sideOutputLateData(outputTag)
      .minBy(1)
    //.reduce((data1, data2) => (data1._1, data1._2.min(data2._2)))

    dataStream.print("in")
    val sideOutPutStream = result.getSideOutput(outputTag)
    sideOutPutStream.print("窗口watermark和allowedLateness之后依然迟到的流数据:")
    result.print("out")

    env.execute("TransformTest")
  }
}
