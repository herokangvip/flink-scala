package com.hk.orderTest

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, UUID}
import java.util.concurrent.TimeUnit

import com.hk.transformTest.Sensor
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
 * 电商双11监控大屏
 */
object OrderAnalysy {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env.addSource(new OrderSource())
      .assignAscendingTimestamps(_.payTime)

    val outputTag = new OutputTag[OrderEvent]("tt")
    val totalSplitStream = dataStream.process(new OrderSplitFunction(outputTag))
    val groupSplitStream = totalSplitStream.getSideOutput(outputTag)

    //dataStream.print("1")
    //聚合总单量及总金额
    val totalStream = totalSplitStream
      .map(data => {
        (1, data)
      })
      .keyBy(_._1)
      .timeWindow(Time.seconds(1))
      .allowedLateness(Time.minutes(1))
      .process(new TotalDataFun())
    totalStream.print("1")
    totalStream.addSink(new OrderMysqlSink())

    //分组聚合每个省份的订单金额和订单量
    val groupStream = groupSplitStream
      .keyBy(_.province)
      .timeWindow(Time.seconds(1))
      .allowedLateness(Time.minutes(1))
      .process(new GroupDataFuc())
    groupStream.print("2")
    groupStream.addSink(new GroupOrderMysqlSink())

    env.execute("-")

  }
}

case class OrderEvent(orderId: String, userId: String, province: String, money: BigDecimal, payTime: Long)

case class OrderTotalResult() {
  private var orderDay: Int = 0
  private var totalNum: Long = 0
  private var totalMoney: BigDecimal = 0.0

  def getOrderDay: Int = {
    this.orderDay
  }

  def setOrderDay(orderDay: Int): Unit = {
    this.orderDay = orderDay
  }

  def getTotalNum: Long = {
    this.totalNum
  }

  def setTotalNum(totalNum: Long): Unit = {
    this.totalNum = totalNum
  }

  def getTotalMoney: BigDecimal = {
    this.totalMoney
  }

  def setTotalMoney(totalMoney: BigDecimal): Unit = {
    this.totalMoney = totalMoney
  }

  /*  override def toString() {
      println("day：" + this.day + "，totalNum：" + this.totalNum + ",totalMoney:" + this.totalMoney);
    }*/
  override def toString: String = "day：" + this.orderDay + "，totalNum：" + this.totalNum + ",totalMoney:" + this.totalMoney
}

/**
 * 分组情况下各省的下单量和订单总金额，输出样例类
 */
case class OrderGroupTotalResult() {
  var orderDay: Int = 0
  var province: String = ""
  var totalNum: Long = 0
  var totalMoney: BigDecimal = 0.0

  def getOrderDay: Int = {
    this.orderDay
  }

  def setOrderDay(orderDay: Int): Unit = {
    this.orderDay = orderDay
  }

  def getTotalNum: Long = {
    this.totalNum
  }

  def setTotalNum(totalNum: Long): Unit = {
    this.totalNum = totalNum
  }

  def getTotalMoney: BigDecimal = {
    this.totalMoney
  }

  def setTotalMoney(totalMoney: BigDecimal): Unit = {
    this.totalMoney = totalMoney
  }

  def getProvince: String = {
    this.province
  }

  def setProvince(province: String): Unit = {
    this.province = province
  }
}

class OrderSplitFunction(tag: OutputTag[OrderEvent]) extends ProcessFunction[OrderEvent, OrderEvent] {
  override def processElement(order: OrderEvent, context: ProcessFunction[OrderEvent, OrderEvent]#Context, collector: Collector[OrderEvent]): Unit = {
    context.output(tag, order)
    collector.collect(order)
  }
}

class GroupOrderMysqlSink() extends RichSinkFunction[util.HashMap[Int, OrderGroupTotalResult]] {
  //定义sql链接，预编译器
  var conn: Connection = _
  //插入
  var insertStmt: PreparedStatement = _
  //更新
  var updateStmt: PreparedStatement = _
  var findStmt: PreparedStatement = _

  //初始化
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/mfcq?useSSL=false&characterEncoding=utf8&serverTimezone=UTC", "root", "root")
    insertStmt = conn.prepareStatement("insert into order_group (orderDay,province,totalNum,totalMoney) values (?,?,?,?)")
    updateStmt = conn.prepareStatement("update order_group set totalNum=totalNum+?,totalMoney=totalMoney+? where orderDay=? and province=?")
    findStmt = conn.prepareStatement("select count(*) from order_group where orderDay=? and province=?")
  }

  override def invoke(map: util.HashMap[Int, OrderGroupTotalResult], context: Context[_]): Unit = {
    //更新
    //updateStmt.setDouble(1, value.temperature)
    //updateStmt.setString(2, value.id)
    val keys = map.keySet()
    val iter = keys.iterator()
    while (iter.hasNext) {
      val key = iter.next()
      val value = map.get(key)
      findStmt.setInt(1, value.getOrderDay)
      findStmt.setString(2, value.getProvince)
      val findRes: ResultSet = findStmt.executeQuery()
      findRes.next()
      val dbNum = findRes.getInt(1)
      //print("查询数量:" + dbNum)
      //print("key:" + key + ",value:" + value)
      if (dbNum == 0) {
        //插入
        insertStmt.setInt(1, value.getOrderDay)
        insertStmt.setString(2, value.getProvince)
        insertStmt.setLong(3, value.getTotalNum)
        insertStmt.setBigDecimal(4, java.math.BigDecimal.valueOf(value.getTotalMoney.doubleValue()))
        insertStmt.execute()
      } else {
        //更新
        //插入
        updateStmt.setLong(1, value.getTotalNum)
        updateStmt.setBigDecimal(2, value.getTotalMoney.bigDecimal)
        updateStmt.setInt(3, value.getOrderDay)
        updateStmt.setString(4, value.getProvince)
        updateStmt.executeUpdate()
      }

    }
  }

  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    findStmt.close()
    conn.close()
  }
}

class OrderMysqlSink() extends RichSinkFunction[util.HashMap[Int, OrderTotalResult]] {
  //定义sql链接，预编译器
  var conn: Connection = _
  //插入
  var insertStmt: PreparedStatement = _
  //更新
  var updateStmt: PreparedStatement = _
  var findStmt: PreparedStatement = _

  //初始化
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/mfcq?useSSL=false&characterEncoding=utf8&serverTimezone=UTC", "root", "root")
    insertStmt = conn.prepareStatement("insert into order_total (orderDay,totalNum,totalMoney) values (?,?,?)")
    updateStmt = conn.prepareStatement("update order_total set totalNum=totalNum+?,totalMoney=totalMoney+? where orderDay=?")
    findStmt = conn.prepareStatement("select count(*) from order_total where orderDay=?")
  }

  override def invoke(map: util.HashMap[Int, OrderTotalResult], context: Context[_]): Unit = {
    //更新
    //updateStmt.setDouble(1, value.temperature)
    //updateStmt.setString(2, value.id)
    val keys = map.keySet()
    val iter = keys.iterator()
    while (iter.hasNext) {
      val key = iter.next()
      val value = map.get(key)
      findStmt.setInt(1, value.getOrderDay)
      val findRes: ResultSet = findStmt.executeQuery()
      findRes.next()
      val dbNum = findRes.getInt(1)
      //print("查询数量:" + dbNum)
      //print("key:" + key + ",value:" + value)
      if (dbNum == 0) {
        //插入
        insertStmt.setInt(1, value.getOrderDay)
        insertStmt.setLong(2, value.getTotalNum)
        //print("===金额:"+java.math.BigDecimal.valueOf(value.getTotalMoney.doubleValue()))
        insertStmt.setBigDecimal(3, java.math.BigDecimal.valueOf(value.getTotalMoney.doubleValue()))
        insertStmt.execute()
      } else {
        //更新
        //插入
        updateStmt.setLong(1, value.getTotalNum)
        updateStmt.setBigDecimal(2, value.getTotalMoney.bigDecimal)
        updateStmt.setInt(3, value.getOrderDay)
        updateStmt.executeUpdate()
      }

    }
  }

  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    findStmt.close()
    conn.close()
  }
}

class GroupDataFuc extends ProcessWindowFunction[OrderEvent, util.HashMap[Int, OrderGroupTotalResult], String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[OrderEvent], out: Collector[util.HashMap[Int, OrderGroupTotalResult]]): Unit = {
    val resultMap = new util.HashMap[Int, OrderGroupTotalResult]()
    val iterator = elements.iterator
    while (iterator.hasNext) {
      val order = iterator.next()
      val date = new Date(order.payTime)
      val sdf = new SimpleDateFormat("yyyyMMdd")
      //val localDateTime = new Date(order.payTime).toInstant.atOffset(ZoneOffset.of("+8")).toLocalDateTime
      //val day = (localDateTime.getYear + "" + localDateTime.getMonth + "" + localDateTime.getDayOfMonth).toInt
      val day = sdf.format(date).toInt
      if (resultMap.containsKey(day)) {
        val entity = resultMap.get(day)
        entity.setOrderDay(day)
        entity.setProvince(entity.getProvince)
        entity.setTotalNum(entity.getTotalNum + 1)
        entity.setTotalMoney(entity.getTotalMoney.+(order.money))
      } else {
        val data: OrderGroupTotalResult = new OrderGroupTotalResult
        data.setOrderDay(day)
        data.setTotalNum(1)
        data.setTotalMoney(order.money)
        data.setProvince(order.province)
        resultMap.put(day, data)
      }
    }
    out.collect(resultMap)
  }
}

/**
 * Map[Int,OrderTotalResult],输出Map，key为年月日拼接的int整形，value为OrderTotalResult
 */
class TotalDataFun extends ProcessWindowFunction[(Int, OrderEvent), util.HashMap[Int, OrderTotalResult], Int, TimeWindow] {
  override def process(key: Int, context: Context, elements: Iterable[(Int, OrderEvent)], out: Collector[util.HashMap[Int, OrderTotalResult]]): Unit = {
    val resultMap = new util.HashMap[Int, OrderTotalResult]()
    val iterator = elements.iterator
    while (iterator.hasNext) {
      val next = iterator.next()
      val order = next._2
      import java.time.ZoneOffset
      val date = new Date(order.payTime)
      val sdf = new SimpleDateFormat("yyyyMMdd")
      //val localDateTime = new Date(order.payTime).toInstant.atOffset(ZoneOffset.of("+8")).toLocalDateTime
      //val day = (localDateTime.getYear + "" + localDateTime.getMonth + "" + localDateTime.getDayOfMonth).toInt
      val day = sdf.format(date).toInt
      if (resultMap.containsKey(day)) {
        val entity = resultMap.get(day)
        entity.setOrderDay(day)
        entity.setTotalNum(entity.getTotalNum + 1)
        entity.setTotalMoney(entity.getTotalMoney.+(order.money))
      } else {
        val data: OrderTotalResult = new OrderTotalResult
        data.setOrderDay(day)
        data.setTotalNum(1)
        data.setTotalMoney(order.money)
        resultMap.put(day, data)
      }
    }
    out.collect(resultMap)
  }
}

class OrderSource() extends SourceFunction[OrderEvent] {
  var running = true
  var max = 2000
  var count = 0
  //省份
  val provinces: Seq[String] = Seq("北京", "北京", "上海", "广州", "深圳", "天津")
  //随机数发生器
  val random: Random = new Random()

  override def run(sourceContext: SourceFunction.SourceContext[OrderEvent]): Unit = {
    while (running && count < max) {
      val data = OrderEvent(UUID.randomUUID().toString, UUID.randomUUID().toString, provinces {
        random.nextInt(provinces.size
        )
      }, random.nextDouble().formatted("%.2f").toDouble+10, System.currentTimeMillis())
      sourceContext.collect(data)
      TimeUnit.MILLISECONDS.sleep(200L)
      count += 1
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}
