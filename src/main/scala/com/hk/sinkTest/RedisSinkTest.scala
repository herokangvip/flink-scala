package com.hk.sinkTest

import com.hk.transformTest.Sensor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig.Builder
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
  * Description: 
  *
  * @author heroking
  * @version 1.0.0
  */
object RedisSinkTest {
  def main(args: Array[String]) {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //flatMap可以完成map和filter的操作，map和filter有明确的语义，转换和过滤更加直白
    val dataFromFile: DataStream[String] = env.readTextFile("E:\\workspace\\flink-scala\\src\\main\\resources\\sensors.txt")
    val dataStream = dataFromFile.map(data => {
      val array = data.split(",")
      Sensor(array(0).trim, array(1).trim.toLong, array(2).trim.toDouble)
    })


    //数据输出到redis
    val redisConf = new Builder().setHost("localhost")
      .setPort(6379)
      .build()
    val sink = new RedisSink(redisConf,new MyRedisMapper())
    dataStream.addSink(sink)


    env.execute("redis test sink")
  }

}

class MyRedisMapper() extends RedisMapper[Sensor] {
  //定义保存redis时的命令
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.SET)
  }

  override def getKeyFromData(t: Sensor): String = {
    t.temperature.toString
  }

  override def getValueFromData(t: Sensor): String = "sensor:" + t.id
}
