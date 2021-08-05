package com.bigdata.sinktest

import com.bigdata.apitest.SensorReading
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object SinkTest {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
//    val inputPath = "src/main/resources/sensor.txt"
//    val stream = environment.readTextFile(inputPath)
    val stream = environment.socketTextStream("localhost", 7777)

    val dataStream = stream.map(data => {
      val arr = data.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    // 样例
//    dataStream.map(data=>{
//      (data.id, data.temperature)
//    }).keyBy(_._1) // 按照二元组的第一个元素(id)进行分组
//      .window(TumblingEventTimeWindows.of(Time.seconds(15)))    // 滚动操作
//      .window(SlidingProcessingTimeWindows.of(Time.seconds(15), Time.seconds(3)))    // 滑动操作
//      .window(EventTimeSessionWindows.withGap(Time.seconds(10)))    // 会话窗口

    // 每15秒统计一次，窗口内各传感器所有温度的最小值, 以及最新的时间戳
    dataStream.map(data=>{
      (data.id, data.temperature, data.timestamp)
    }).keyBy(_._1)
      .timeWindow(Time.seconds(15))
      .reduce((curRes, newData) => (curRes._1, curRes._2.min(newData._2), newData._3))
//      .reduce(new MyReducer[(String, Long, Double)])

    environment.execute(this.getClass.getSimpleName)
  }
}

class MyReducer extends ReduceFunction[(String, Long, Double)]{
  override def reduce(t: (String, Long, Double), t1: (String, Long, Double)): (String, Long, Double) = {
    (t._1, t._2.min(t1._2), t1._3)
  }
}