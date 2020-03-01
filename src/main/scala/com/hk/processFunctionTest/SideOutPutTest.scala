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
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //flatMap可以完成map和filter的操作，map和filter有明确的语义，转换和过滤更加直白
    val dataFromFile: DataStream[String] = env.readTextFile("E:\\workspace\\flink-scala\\src\\main\\resources\\sensors.txt")
    val dataStream: DataStream[Sensor] = dataFromFile.map(data => {
      val array = data.split(",")
      new Sensor(array(0).trim, array(1).trim.toLong, array(2).trim.toDouble)
    })
    //泛型为侧输出流要输出的数据格式
    val tag: OutputTag[Sensor] = new OutputTag[Sensor]("hot")
    val result = dataStream
      .process(new HotAlarm(tag))

    //获取侧输出流信息
    val sideOutPut: DataStream[Sensor] = result.getSideOutput(tag)
    sideOutPut.print("侧输出流:")

    result.print("out:")
    env.execute("TransformTest")
  }
}

//第二个参数是主输出流将要输出的数据类型
/**
  * 如果温度过高，输出报警信息到侧输出流
  */
class HotAlarm(alarmOutPutStream:OutputTag[Sensor]) extends ProcessFunction[Sensor, Sensor] {
  override def processElement(sensor: Sensor, context: ProcessFunction[Sensor, Sensor]#Context, collector: Collector[Sensor]): Unit = {
    if (sensor.temperature > 0.5) {
      context.output(alarmOutPutStream, sensor)
    } else {
      collector.collect(sensor)
    }
  }
}
