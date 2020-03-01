package com.hk.windowTest

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector


object TestUtcTime {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "localhost:9092")
    prop.setProperty("group.id", "test-group")
    prop.setProperty("key-deserializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.setProperty("value-deserializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.setProperty("auto.offset.reset", "latest")
    val consumer011 = new FlinkKafkaConsumer011[String]("test-topic", new SimpleStringSchema(), prop)
    val dataStream = env.addSource(consumer011)

    val dd: DataStream[MyTime] = dataStream.map(data => {
      MyTime(data.toLong)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[MyTime](Time.seconds(1)) {
        override def extractTimestamp(t: MyTime): Long = t.timestamp*1000
      })

    val d2: KeyedStream[MyTime, Tuple] = dd.keyBy("timestamp")
    val d3: AllWindowedStream[MyTime, TimeWindow] = d2.windowAll(TumblingEventTimeWindows.of(Time.days(1), Time.hours(16)))

    val d4: AllWindowedStream[MyTime, TimeWindow] = d3.trigger(new UtcTrigger())

    val value: DataStream[String] = d4.process(new UtcProcess())


    //      .keyBy(_.timestamp)
//      //如果按天开窗，由于时区问题，第二个参数需要设置16小时，这样窗口才是从0点到24点
//      .windowAll(TumblingEventTimeWindows.of(Time.days(1),Time.hours(16)))
//      .trigger(new UtcTrigger())
//      .process(new UtcProcess())

    //dd.print()


    //dataStream.print().setParallelism(1)
    env.execute("SourceFromKafka")
  }

  class UtcTrigger() extends Trigger[MyTime, TimeWindow] {
    override def onElement(t: MyTime, l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE

    override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    override def onEventTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE

    override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {}
  }

  case class UtcProcess() extends ProcessAllWindowFunction[MyTime,String,TimeWindow]{
    override def process(context: Context, elements: Iterable[MyTime], out: Collector[String]): Unit = {
      val t = elements.iterator.next().timestamp.toString
      var start = context.window.getStart
      var end = context.window.getEnd
      println(t+"=====")
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      println("start:"+sdf.format(new Date(start)))
      println("end:"+sdf.format(new Date(end)))
      out.collect(t+"=====")
    }

    override def clear(context: Context): Unit = {
      println("窗口clear")
    }
  }

}
