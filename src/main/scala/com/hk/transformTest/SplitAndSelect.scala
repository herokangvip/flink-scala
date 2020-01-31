package com.hk.transformTest

import org.apache.flink.streaming.api.scala._

/**
  * Description: 将流拆分成新的流
  *
  * @author heroking
  * @version 1.0.0
  */
object SplitAndSelect {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //flatMap可以完成map和filter的操作，map和filter有明确的语义，转换和过滤更加直白
    val dataFromFile: DataStream[String] = env.readTextFile("E:\\workspace\\flink-scala\\src\\main\\resources\\sensors.txt")
    val dataStream: DataStream[Sensor] = dataFromFile.map(data => {
      val array = data.split(",")
      new Sensor(array(0).trim, array(1).trim.toLong, array(2).trim.toDouble)
    })

    //使用split将流数据打上不同标记，结合select方法真正的分离出新的流DataStream
    //已废弃，参考SideOutPutTest，使用ProcessFunction实现侧输出流
    val splitStream = dataStream.split(data => {
      if (data.temperature >= 30) {
        Seq("high")
      }
      else if (data.temperature >= 20 && data.temperature < 30) {
        Seq("mid")
      }
      else
        Seq("low")
    })
    val high = splitStream.select("high")
    val mid = splitStream.select("mid")
    val low = splitStream.select("low")

    high.print()

    env.execute("test")
  }
}
