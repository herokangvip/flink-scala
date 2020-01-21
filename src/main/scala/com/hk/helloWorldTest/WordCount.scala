package com.hk.helloWorldTest

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.core.fs.FileSystem

/**
  * Description: 批处理--离线wordCount
  *
  * @author heroking
  * @version 1.0.0
  */
/*
 * =========================== 维护日志 ===========================
 * 2020-01-20 13:11  heroking 新建代码
 * =========================== 维护日志 ===========================
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    //创建批处理执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    //读取文件
    val inputPath = "D:\\a.txt"
    val dataSet = env.readTextFile(inputPath)

    val result = dataSet.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)
    result.print()

  }
}
