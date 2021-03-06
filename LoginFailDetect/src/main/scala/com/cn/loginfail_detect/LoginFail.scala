package com.cn.loginfail_detect

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


// 输入的登录时间样例类
case class LoginEvent(usetId: Long, ip: String, eventType: String, timestamp:Long)
// 输出报警信息样例类
case class LoginFailWarning(userId: Long, firstFailTime: Long, lastFailTime: Long, waringMsg: String)


object LoginFail {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据
    val resource = getClass.getResource("/LoginLog.csv")
    val inputStream = env.readTextFile(resource.getPath)

    // 转换成样例类类型，并提取时间戳和watermark
    val loginEventStream = inputStream
      .map(data => {
        val arr = data.split(",")
        LoginEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
        override def extractTimestamp(element: LoginEvent): Long = element.timestamp * 1000L
      })

    // 进行判断和检测，如果2秒之内连续登录失败，输出报警信息
    val loginFailWarningStream = loginEventStream
      .keyBy(_.usetId)
      .process(new LoginFailWarningResult(2))

    loginFailWarningStream.print()
    env.execute("login_fail_detect_job")
  }
}


class LoginFailWarningResult(failTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]{
  // 定义状态，保存当前所有的登录失败事件，保存定时器的时间戳
  lazy val loginFailListState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginfail-list", classOf[LoginEvent]))
  lazy val timerTsState:ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#Context, out: Collector[LoginFailWarning]): Unit = {
    // 判断当前登录事件是成功还是失败
    if(value.eventType == "fail"){
      loginFailListState.add(value)
      // 如果没有定时器，那么注册一个2秒后的定时器
      if(timerTsState.value() == 0){
        val ts = value.timestamp * 1000L + 2000L
        ctx.timerService().registerEventTimeTimer(ts)
        timerTsState.update(ts)
      }
    } else {
      // 如果是成功，那么直接清空状态和定时器，重新开始
      ctx.timerService().deleteEventTimeTimer(timerTsState.value())
      loginFailListState.clear()
      timerTsState.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#OnTimerContext, out: Collector[LoginFailWarning]): Unit = {
    val allLoginFailList: ListBuffer[LoginEvent] = new ListBuffer[LoginEvent]()
    val iter = loginFailListState.get().iterator()
    while(iter.hasNext){
      allLoginFailList += iter.next()
    }

    // 判断登录失败事件的个数，如果超过了上限，报警
    if(allLoginFailList.length >= failTimes){
      out.collect(
        LoginFailWarning(
          allLoginFailList.head.usetId,
          allLoginFailList.head.timestamp,
          allLoginFailList.last.timestamp,
          "login fail in 2s for" + allLoginFailList.length + "times"

        ))
    }

    // 清空状态
    loginFailListState.clear()
    timerTsState.clear()
  }
}