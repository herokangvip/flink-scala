package com.hk.transformTest

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._

/**
  * Description: 
  *
  * @author heroking
  * @version 1.0.0
  */
case class Sensor(id: String, timestamp: Long, temperature: Double)

object TransformTest {
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
      .keyBy(_.id)//keyBy函数会对数据进行hash重分区，dataStream流还是一个流只是做了重分区
      .sum("temperature")
      //.reduce( (x,y) => {Sensor(x.id, x.timestamp+1, y.temperature+1)})
    //聚合函数sum、reduce、max、min等职能用在keyBy后，keyBy相当于groupBy
    //可以充分利用并行
    //sum测试输出
    /*Sensor(1,7282164761,12.2)
    Sensor(2,7282164762,22.2)
    Sensor(3,7282164763,32.2)
    Sensor(4,7282164764,42.2)
    Sensor(1,7282164761,64.4)
    Sensor(1,7282164761,126.60000000000001)*/

    //reduce测试输出
    /*Sensor(1,7282164761,12.2)
    Sensor(2,7282164762,22.2)
    Sensor(3,7282164763,32.2)
    Sensor(4,7282164764,42.2)
    Sensor(1,7282164762,53.2)
    Sensor(1,7282164763,63.2)*/
    dataStream.print()

    env.execute("TransformTest")
  }

}
