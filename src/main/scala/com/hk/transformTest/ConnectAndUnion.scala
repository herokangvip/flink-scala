package com.hk.transformTest

import org.apache.flink.streaming.api.scala._

/**
  * Description: 将流合并,先将流拆分在测试流的合并
  *
  * @author heroking
  * @version 1.0.0
  */
object ConnectAndUnion {
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

    //测试流的合并。模拟一个传感器报警的信息
    val warning = high.map(data => (data.id, data.temperature))
      .connect(mid)

    //union可以合并多个流，但是必须是类型一样的流
    //high.union(mid).union(low)


    //connect合并的流可以是类型不同的流(且只能用于两个流，connect后不能再connect)，
    // 可以对两个流进行map等操作
    val resultStream = warning.map(high => {
      (high._1, "error")
    }, mid => {
      (mid.id, "warn")
    })

    resultStream.print()
    /*(3,error)
    (4,error)
    (1,error)
    (1,error)
    (2,warn)*/

    env.execute("test")


  }
}
