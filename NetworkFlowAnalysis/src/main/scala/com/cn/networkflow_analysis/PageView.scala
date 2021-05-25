package com.cn.networkflow_analysis

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

// 定义输入数据样例类
case class UserBehavior(userId: Long, itemId: Long, categroy: Int, behavior:String, timestamp: Long)
// 定义输出PV统计的样例类
case class PvCount( windowEnd: Long, count: Long)

object PageView {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 从文件读取数据
    val resource = getClass.getResource("/UserBehavior.csv")
    val inputStream: DataStream[String] = env.readTextFile(resource.getPath)

    // 转换成样例类类型并提取时间戳和watermark
    val dataStream: DataStream[UserBehavior] = inputStream
      .map(data => {
        val arr = data.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    val pvStream = dataStream
      .filter(_.behavior == "pv")
      .map(data => ("pv", 1L)) // 定义一个pv字符串作为分组的dummy key
      .keyBy(_._1) // 所有数据会被聚合到同一个组
      .timeWindow(Time.hours(1))  // 1小时滚动窗口
      .aggregate(new PvCountAgg() , new PvCountWindowResult())

    pvStream.print()

    env.execute("pv_job")

  }
}

// 自定义预聚合函数
class  PvCountAgg() extends AggregateFunction[(String, Long), Long, Long]{
  override def createAccumulator(): Long = 0L

  override def add(value: (String, Long), accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

// 自定义窗口函数
class PvCountWindowResult() extends WindowFunction[Long, PvCount, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PvCount]): Unit = {
    out.collect(PvCount(window.getEnd, input.head))
  }
}