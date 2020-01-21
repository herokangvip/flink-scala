package com.hk.sourceTest

import com.hk.transformTest.Sensor
import org.apache.flink.streaming.api.scala._

/**
  * Description: 
  *
  * @author heroking
  * @version 1.0.0
  */
object SourceFromListOrFile {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //从集合读
    val dataStream = env.fromCollection(List {
      new Sensor("1", 7282164761L, 12.2)
      new Sensor("2", 7282164762L, 22.2)
      new Sensor("3", 7282164763L, 32.2)
      new Sensor("4", 7282164764L, 42.2)
      new Sensor("5", 7282164765L, 52.2)
      new Sensor("6", 7282164766L, 62.2)
    })
    //从文件读
    //env.readTextFile("D\\:a.txt")

    dataStream.filter(a => a.temperature >= 20)

    dataStream.print().setParallelism(1)
    env.execute("SourceTest")

  }
}
