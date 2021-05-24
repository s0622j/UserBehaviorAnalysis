package com.cn.networkflow_analysis

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

// 输入数据样例类
case class ApacheLogEvent( ip: String, userId: String, timestamp: Long, method: String, url: String)

// 窗口聚合结果样例类
case class PageViewCount( url: String, windowEnd: Long, count: Long )

object HotPagesNetworkFlow {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据，转换成样例类并提取时间戳和watermark
    val inputStream = env.readTextFile("D:\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log")
    val dataStream = inputStream
      .map( data =>{
        val arr = data.split(" ")
        // 对事件时间进行转换，得到时间戳
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val ts = simpleDateFormat.parse(arr(3)).getTime

        ApacheLogEvent(arr(0), arr(1), ts, arr(5), arr(6))
      } )
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.minutes(1)) {
        override def extractTimestamp(element: ApacheLogEvent): Long = element.timestamp
      })

    // 进行开窗聚合，以及排序输出
    val aggStream = dataStream
      .filter(_.method == "GET")
//      .filter(data => {
//        val pattern = "^((?!\\.(css|js)$).)*$".r(
//          pattern findFirstIn data.url
//        ).nonEmpty
//      })
//      .keyBy("url")
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .aggregate(new PageCountAgg(), new PageViewCountWindowResult())

    val resultStream = aggStream
      .keyBy(_.windowEnd)
      .process(new TopNHotPages(3))

    resultStream.print()
    env.execute("hot_pages_job")
  }
}

class  PageCountAgg() extends  AggregateFunction[ApacheLogEvent, Long, Long]{
  override def createAccumulator(): Long = 0L

  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class  PageViewCountWindowResult() extends WindowFunction[Long, PageViewCount, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PageViewCount]): Unit = {
    out.collect(PageViewCount(key, window.getEnd, input.iterator.next()))
  }
}

class  TopNHotPages(n: Int) extends KeyedProcessFunction[Long, PageViewCount, String]{
  lazy val pageViewCountListState: ListState[PageViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[PageViewCount]("PageViewCount-list", classOf[PageViewCount]))

  override def processElement(value: PageViewCount, ctx: KeyedProcessFunction[Long, PageViewCount, String]#Context, out: Collector[String]): Unit = {

    pageViewCountListState.add(value)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val allPageViewCounts: ListBuffer[PageViewCount] = ListBuffer()
    val iter = pageViewCountListState.get().iterator()
    while (iter.hasNext){
      allPageViewCounts += iter.next()
    }

    // 提前清空状态
    pageViewCountListState.clear()

    // 按照访问量排序并输出top n
    val sortedPageViewCounts = allPageViewCounts.sortWith(_.count > _.count).take(n)


    // 将排名信息格式化成String,便于打印输出可视化展示
    val result: StringBuilder = new StringBuilder
    result.append("窗口结束时间：").append(new Timestamp(timestamp - 1) ).append("\n")
    //遍历结果列表中的每个ItemViewCount,输出到一行
    for( i <- sortedPageViewCounts.indices ){
      val currentItemViewCount = sortedPageViewCounts(i)
      result.append("NO").append(i + 1).append(": \t")
        .append("页面URL = ").append(currentItemViewCount.url).append("\t")
        .append("热门度 = ").append(currentItemViewCount.count).append("\n")
    }

    result.append("\n==============================================\n\n")

    Thread.sleep(1000)

    out.collect(result.toString())
  }
}