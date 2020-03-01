package com.hk.windowTest

import java.{lang, util}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.evictors.{CountEvictor, Evictor}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue
import org.apache.flink.util.Collector

case class MyTime(timestamp: Long)

object TestTriggerAndEvictor {

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
        override def extractTimestamp(t: MyTime): Long = t.timestamp * 1000
      })


    val d2: KeyedStream[MyTime, Long] = dd.keyBy(_.timestamp)

    /*val d3: AllWindowedStream[MyTime, TimeWindow] = d2.windowAll(
      TumblingEventTimeWindows.of(Time.days(1), Time.hours(16)))*/
    val d3: AllWindowedStream[MyTime, TimeWindow] = d2.windowAll(
      TumblingEventTimeWindows.of(Time.seconds(10)))

    val d4: AllWindowedStream[MyTime, TimeWindow] = d3.trigger(new UtcTrigger())

    val d5: AllWindowedStream[MyTime, TimeWindow] = d4.evictor(new MyEvictor())

    val value: DataStream[String] = d5.process(new UtcProcess())
    //      .keyBy(_.timestamp)
    //      //如果按天开窗，由于时区问题，第二个参数需要设置16小时，这样窗口才是从0点到24点
    //      .windowAll(TumblingEventTimeWindows.of(Time.days(1),Time.hours(16)))
    //      .trigger(new UtcTrigger())
    //      .process(new UtcProcess())

    //dd.print()
    //dataStream.print().setParallelism(1)
    env.execute("SourceFromKafka")
  }
}

/**
 * 窗口触发器，决定了窗口什么时候使用窗口函数处理窗口内元素。每个窗口分配器都带有一个默认的触发器。
 * TriggerResult四个值：CONTINUE、FIRE、FIRE_AND_PURGE、PURGE；
 * FIRE、FIRE_AND_PURGE区别：FIRE触发计算不清空窗口数据，FIRE_AND_PURGE：触发计算并清空窗口数据；
 * 如果后面的Function等计算用户自己增量维护状态，可以只接受增量数据则使用FIRE_AND_PURGE；
 * FIRE之后的Function中会受到整个窗口的数据而FIRE_AND_PURGE只会收到增量数据，特别是在一些大窗口大数据量案例中不清理数据可能会oom
 * Flink带有一些内置触发器:
 * EventTimeTrigger 窗口默认的Triiger，根据 watermarks 度量的事件时间进度进行触发。
 * ProcessingTimeTrigger 窗口默认的Triiger，基于处理时间触发。
 * CountTrigger 一旦窗口中的元素数量超过给定限制就会触发，FIRE不清理数据。
 * ContinuousEventTimeTrigger 每隔一段时间触发，FIRE不清理数据。
 * PurgingTrigger 将其作为另一个触发器的参数，并将其转换为带有清除功能(transforms it into a purging one)。
 */
class UtcTrigger() extends Trigger[MyTime, TimeWindow] {
  //当每个元素被添加窗口时调用
  override def onElement(t: MyTime, l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
    println("触发器onElement")
    TriggerResult.FIRE_AND_PURGE
  }

  //当注册的处理时间计时器被触发时调用
  override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
    println("触发器onProcessingTime")
    TriggerResult.CONTINUE
  }

  //当注册的事件时间计时器被触发时调用
  override def onEventTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
    println("触发器onEventTime")
    TriggerResult.FIRE_AND_PURGE
  }

  //在清除（removal）窗口时调用
  override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {
    println("触发器clear")
  }
}

/**
 * Flink 窗口模型还允许在窗口分配器和触发器之外指定一个可选的驱逐器(Evictor)。
 * 可以使用 evictor(...) 方法来完成。
 * 驱逐器能够在触发器触发之后，窗口函数使用之前或之后从窗口中清除元素。
 * evictBefore()在窗口函数之前使用。而 evictAfter() 在窗口函数之后使用。
 * 在使用窗口函数之前被逐出的元素将不被处理。
 * Flink带有三种内置驱逐器:
 * CountEvictor：在窗口维护用户指定数量的元素，如果多于用户指定的数量，从窗口缓冲区的开头丢弃多余的元素。
 * DeltaEvictor：使用 DeltaFunction 和一个阈值，来计算窗口缓冲区中的最后一个元素与其余每个元素之间的差值，并删除差值大于或等于阈值的元素。
 * TimeEvictor：以毫秒为单位的时间间隔（interval）作为参数，对于给定的窗口，找到元素中的最大的时间戳max_ts，并删除时间戳小于max_ts - interval的所有元素。
 * 默认情况下，所有内置的驱逐器在窗口函数之前使用。指定驱逐器可以避免预聚合(pre-aggregation)，因为窗口内所有元素必须在窗口计算之前传递给驱逐器。
 * Flink 不保证窗口内元素的顺序。这意味着虽然驱逐器可以从窗口开头移除元素，但这些元素不一定是先到的还是后到的。
 */
class MyEvictor() extends Evictor[MyTime, TimeWindow] {
  override def evictBefore(iterable: lang.Iterable[TimestampedValue[MyTime]], i: Int, w: TimeWindow, evictorContext: Evictor.EvictorContext): Unit = {
    val ite: util.Iterator[TimestampedValue[MyTime]] = iterable.iterator()
    while (ite.hasNext) {
      val elment: TimestampedValue[MyTime] = ite.next()
      //指定事件时间获取到的就是事件时间
      println("驱逐器获取到的时间：" + elment.getTimestamp)
      //模拟去掉非法参数数据
      if (elment.getValue.timestamp <= 0) {
        ite.remove()
      }
    }
  }

  override def evictAfter(iterable: lang.Iterable[TimestampedValue[MyTime]], i: Int, w: TimeWindow, evictorContext: Evictor.EvictorContext): Unit = {

  }
}

class UtcProcess() extends ProcessAllWindowFunction[MyTime, String, TimeWindow] {
  override def process(context: Context, elements: Iterable[MyTime], out: Collector[String]): Unit = {
    val t = elements.iterator.next().timestamp.toString
    var start = context.window.getStart
    var end = context.window.getEnd
    println(t + "=====processFunction")
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    println("start:" + sdf.format(new Date(start)))
    println("end:" + sdf.format(new Date(end)))
    out.collect(t + "=====processFunction")
  }

  override def clear(context: Context): Unit = {
    println("窗口clear")
  }
}
