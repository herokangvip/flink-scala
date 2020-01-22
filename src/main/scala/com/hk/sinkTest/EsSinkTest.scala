package com.hk.sinkTest

import java.util

import com.hk.transformTest.Sensor
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{RequestIndexer, ElasticsearchSinkFunction}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig.Builder
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

/**
  * Description: 
  *
  * @author heroking
  * @version 1.0.0
  */
object EsSinkTest {
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


    //数据输出到es
    val httpHost = new util.ArrayList[HttpHost]()
    httpHost.add(new HttpHost("localhost",9200))
    val esBuilder = new ElasticsearchSink.Builder[Sensor](httpHost,new ElasticsearchSinkFunction[Sensor] {
      override def process(t: Sensor, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        val map = new util.HashMap[String,String]()
        map.put("id",t.id)
        map.put("timestamp",t.timestamp.toString)
        map.put("temperature",t.temperature.toString)
        //创建indexRequest，准备发送数据
        val indexRequest = Requests.indexRequest()
          .index("sensor")
          .`type`("type")
          .source(map)
        //发送数据
        requestIndexer.add(indexRequest)
      }
    })

    dataStream.addSink(esBuilder.build())


    env.execute("redis test sink")
  }

}
