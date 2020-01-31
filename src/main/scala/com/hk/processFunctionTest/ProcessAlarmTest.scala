package com.hk.processFunctionTest

import java.util.Properties
import java.util.concurrent.ThreadPoolExecutor

import com.hk.transformTest.Sensor
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector


/**
  * Description: 10s内任意两个相邻的温度连续上升则报警
  * 此例子为有状态的流式处理
  *
  * @author heroking
  * @version 1.0.0
  */
object ProcessAlarmTest {
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


    val dataStream: DataStream[Sensor] = kafkaDataStream.map(data => {
      val array = data.split(",")
      new Sensor(array(0).trim, array(1).trim.toLong, array(2).trim.toDouble)
    })

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
    val result = waterMarkDataStream
      .keyBy(_.id)
      .process(new MyKeyedProcessFunctionAlarm())

    waterMarkDataStream.print("in:")
    result.print("out:")

    env.execute("TransformTest")
  }
}

/**
  * 构造函数三个，分别是keyBy的key和in、out
  */
class MyKeyedProcessFunctionAlarm extends KeyedProcessFunction[String, Sensor, String] {
  //上一个数据的温度
  val lastTempValueStateDescriptor = new ValueStateDescriptor[Double]("lastTemp", classOf[Double])
  lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(lastTempValueStateDescriptor)
  //定义一个状态用来保存定时器注册时的时间戳
  val currentTimerValueStateDescriptor = new ValueStateDescriptor[Long]("currentTimer", classOf[Long])
  lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState(currentTimerValueStateDescriptor)

  override def processElement(sensor: Sensor, context: KeyedProcessFunction[String, Sensor, String]#Context, collector: Collector[String]): Unit = {
    //先取出上一个数据的温度值,并更细state为下一个温度值
    val preTemp = lastTemp.value()
    lastTemp.update(sensor.temperature)
    //温度上升则注册定时器,之前没注册过定时器
    if (sensor.temperature > preTemp && currentTimer.value() == 0 && preTemp!=0.0) {
      val nowTime = context.timerService().currentProcessingTime()
      //定时器构造函数时间戳按1970-0-0计算,10秒报警
      context.timerService().registerProcessingTimeTimer(nowTime + 10000)
      //注册的定时器时间添加到state
      currentTimer.update(nowTime + 10000)
    }else if(sensor.temperature < preTemp || preTemp==0.0){
      //如果温度下降
      context.timerService().deleteProcessingTimeTimer(currentTimer.value())
      //删除定时器后清空state
      currentTimer.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Sensor, String]#OnTimerContext, out: Collector[String]): Unit = {
    //输出报警信息
    out.collect("sensor_"+ctx.getCurrentKey+",温度连续上升")
    currentTimer.clear()
  }
}
