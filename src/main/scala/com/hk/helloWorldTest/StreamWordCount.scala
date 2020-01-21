package com.hk.helloWorldTest

import org.apache.flink.streaming.api.scala._

/**
  * Description: 流处理---wordCount
  *
  * @author heroking
  * @version 1.0.0
  */
/*
 * =========================== 维护日志 ===========================
 * 2020-01-20 13:56  heroking 新建代码
 * =========================== 维护日志 ===========================
 */
object StreamWordCount {
  def main(args: Array[String]) {
    /*val params = ParameterTool.fromArgs(args)
    val host: String = params.get("host")
    val port: Int = params.getInt("port")*/
    //创建流处理执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //接收一个socket文本流,可以安装netcat，使用cmd执行nc -l -p 7777创建会话窗口
    val dataStrem = env.socketTextStream("localhost", 7777)
    //对每条数据处理
    val wordCountDataStream = dataStrem.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0) //会hash重分区
      .sum(1) //keyBy后才能使用聚合算子如reduce

    wordCountDataStream.print().setParallelism(1)
    //启动execute
    env.execute("stream-word-count-job")
  }

}
