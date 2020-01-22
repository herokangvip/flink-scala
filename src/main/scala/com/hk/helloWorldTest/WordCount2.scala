package com.hk.helloWorldTest

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Description: 流处理从文件读，有限的数据就变成了批处理，处理完数据后会自动关闭
  *
  * @author heroking
  * @version 1.0.0
  */
/*
 * =========================== 维护日志 ===========================
 * 2020-01-20 13:11  heroking 新建代码
 * =========================== 维护日志 ===========================
 */
object WordCount2 {
  def main(args: Array[String]): Unit = {
    //创建批处理执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //读取文件
    val inputPath = "D:\\a.txt"
    val dataSet = env.readTextFile(inputPath)

    val result = dataSet.flatMap(_.split(" ")).map((_, 1)).keyBy(0).sum(1)
    result.print()

    env.execute()

  }
}
