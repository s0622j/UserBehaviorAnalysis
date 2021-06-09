package com.cn.orderpay_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object OrderTimeoutWithoutCep {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 0. 从文件中读取数据
        val resource = getClass.getResource("/OrderLog.csv")
        val orderEventStream = env.readTextFile(resource.getPath)
//    val orderEventStream = env.socketTextStream("localhost",7777)
      .map(data => {
        val arr = data.split(",")
        OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000)
      .keyBy(_.orderId)

    // 自定义ProcessFunction进行复杂事件的检测
    val orderResultStream = orderEventStream
      .keyBy(_.orderId)
      .process( new OrderPayMatchResule() )

    orderResultStream.print("payed")
    orderResultStream.getSideOutput(new OutputTag[OrderResult]("timeout")).print("timeout")

    env.execute("order timeout without cep")

  }
}

// 自定义实现KeyedProcessFunction
class OrderPayMatchResule extends KeyedProcessFunction[Long, OrderEvent, OrderResult]{
  // 定义状态，标识位表示create、pay是否已经来过，定时器事件戳
  lazy val isCreateState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-created", classOf[Boolean]))
  lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-payed", classOf[Boolean]))
  lazy val timeTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("time-ts", classOf[Long]))

  // 定义测输出流标签
  val orderTimeoutOutputTag = new OutputTag[OrderResult]("timeout")
  override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
    // 先拿到当前状态
    val isPayed = isPayedState.value()
    val isCreate = isCreateState.value()
    val timeTs = timeTsState.value()

    // 判断当前事件类型，看是create还是pay
    // 1.来的是create，要继续判断是否pay过
    if(value.eventType == "create"){
      // 1.1 若果已经支付过，正常支付，输出匹配成功的结果
      if(isPayed){
        // 已经处理完毕，清空状态和定时器
        out.collect(OrderResult(value.orderId, "payed successfully"))
        isCreateState.clear()
        isPayedState.clear()
        timeTsState.clear()
        ctx.timerService().deleteEventTimeTimer(timeTs)
      } else {
          // 1.2 如果还没pay过，注册定时器，等待15分钟
        val ts = value.timestamp * 1000L + 900 * 1000L
        ctx.timerService().registerEventTimeTimer(ts)
        // 更新状态
        timeTsState.update(ts)
        isCreateState.update(true)
        //
      }
    } /*2.如果当前来的是pay，要判断是否create过*/else if (value.eventType == "pay"){
      if(isCreate){
        // 2.1 如果已经create过，匹配成功，还要判断一下pay时间是否超过了定时器时间
        if(value.timestamp * 1000L < timeTs){
          //2.1.1 没有超时，正常输出
          out.collect(OrderResult(value.orderId, "payed successfully"))
        } else {
          // 2.1.2 已经超时，输出超时
          ctx.output(orderTimeoutOutputTag, OrderResult(value.orderId,"payed but already timeout"))
        }
        // 只要输出结果，当前order处理已经结束，清空状态和定时器
        isCreateState.clear()
        isPayedState.clear()
        timeTsState.clear()
        ctx.timerService().deleteEventTimeTimer(timeTs)
      } else {
        // 2.2 如果create没来，注册定时器，等到pay的时间就可以
        ctx.timerService().registerEventTimeTimer(value.timestamp * 1000L)
        // 更新状态
        timeTsState.update(value.timestamp * 1000L)
        isPayedState.update(true)
      }
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
    // 定时器触发
    // 1.pay来了，没等到create
    if(isPayedState.value()){
      ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "payed but not found create log"))
    } else {
      // 2.create来了，没有pay
      ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "order timeout"))
    }
    isCreateState.clear()
    isPayedState.clear()
    timeTsState.clear()

  }
}