package com.cn.orderpay_detect


import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object TxMatchWithJoin {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 1. 读取订单事件数据
    val resource1 = getClass.getResource("/OrderLog.csv")
    val orderEventStream = env.readTextFile(resource1.getPath)
      //    val orderEventStream = env.socketTextStream("localhost",7777)
      .map(data => {
      val arr = data.split(",")
      OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
    })
      .assignAscendingTimestamps(_.timestamp * 1000)
      .filter(_.eventType == "pay")
      .keyBy(_.txId)

    // 2. 读取到账事件数据
    val resource2 = getClass.getResource("/ReceiptLog.csv")
    val receiptEventStream = env.readTextFile(resource2.getPath)
      //    val orderEventStream = env.socketTextStream("localhost",7777)
      .map(data => {
      val arr = data.split(",")
      ReceiptEvent(arr(0), arr(1), arr(2).toLong)
    })
      .assignAscendingTimestamps(_.timestamp * 1000)
      .keyBy(_.txId)

    val resultStream = orderEventStream.intervalJoin(receiptEventStream)
      .between(Time.seconds(-3), Time.seconds(5))
      .process(new TxMatchWithJoinResult())

    resultStream.print()
    env.execute("tx match with join job")
  }
}

class  TxMatchWithJoinResult() extends ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent,ReceiptEvent)]{
  override def processElement(left: OrderEvent, right: ReceiptEvent, ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    out.collect((left,right))
  }
}
