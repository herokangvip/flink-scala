package com.hk.windowTest

import com.hk.transformTest.Sensor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Description: 
  *
  * @author heroking
  * @version 1.0.0
  */
object WindowTest {
  def main(args: Array[String]) {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //flatMap可以完成map和filter的操作，map和filter有明确的语义，转换和过滤更加直白
    val dataFromFile: DataStream[String] = env.readTextFile("E:\\workspace\\flink-scala\\src\\main\\resources\\sensors.txt")
    val dataStream: DataStream[Sensor] = dataFromFile.map(data => {
      val array = data.split(",")
      new Sensor(array(0).trim, array(1).trim.toLong, array(2).trim.toDouble)
    })
      //.keyBy(0).sum(2)
      .keyBy(_.id) //keyBy函数会对数据进行hash重分区，dataStream流还是一个流只是做了重分区

      //窗口分类：固定时间窗口、滑动时间窗口、滚动计数窗口，滑动计数窗口，window/timeWindow/countWindow
      //window方法必须在keyBy后才能用，windowAll可以用在之前但一般不用
      .timeWindow(Time.seconds(15),Time.seconds(5))
      //窗口函数分为，增量聚合函数（每条数据实时计算）、全窗口函数（把窗口所有数据收集起来再计算，比如排序）
      .sum("temperature")

    dataStream.print()

    env.execute("TransformTest")
  }
}
