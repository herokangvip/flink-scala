package com.hk.CheckpointTest

import java.util.Properties

import com.hk.transformTest.Sensor
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.SimpleStringSchema


/**
  *
  * @author heroking
  * @version 1.0.0
  */
object CheckPointTest {
  def main(args: Array[String]) {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //开启checkpoint每分钟checkpoint一次
    env.enableCheckpointing(60000)
    //选择checkpoint的状态后端；一般由运维在flink集群配置文件指定
    env.setStateBackend(new FsStateBackend("hdfs://namenode:9000/flink/checkpoints"))
    //设置重启策略，也可以在配置文件配,最大三次重启，每次间隔10s
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,
      org.apache.flink.api.common.time.Time.seconds(10)))
    //设置重启策略，也可以在配置文件配,10s内三次重启，每次间隔1s
    env.setRestartStrategy(RestartStrategies.failureRateRestart(3,
      org.apache.flink.api.common.time.Time.seconds(10),
      org.apache.flink.api.common.time.Time.seconds(1)))
    //默认EXACTLY_ONCE
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //设置checkpoint超时时间超时后放弃本次checkpoint
    env.getCheckpointConfig.setCheckpointTimeout(600000)
    //checkpoint出现异常时是否主动把应用程序job fail掉，默认是true就是checkpoint失败人物也会失败
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    //最大并行checkpoint任务数，checkpoint太频繁会影响性能
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    //两次checkpoint的最小时间间隔ms
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(10000)
    //是否开启checkpoint外部持久化，默认任务失败检查点保存的数据会被删除
    //ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION:即使手动取消任务也不要删除保存点
    //ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION：手动取消任务删除保存点
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

  }
}
