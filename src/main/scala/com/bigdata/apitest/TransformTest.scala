package com.bigdata.apitest

import org.apache.flink.api.common.functions.{FilterFunction, ReduceFunction, RichMapFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object TransformTest {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val inputPath = "src/main/resources/sensor.txt"
    val stream = environment.readTextFile(inputPath)

    // 1. 先转换成样例类类型
    val dataStream = stream.map(data => {
      val arr = data.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    // 2. 分组聚合, 输出每个传感器当前最小值
    val aggStream = dataStream.keyBy(_.id).minBy("temperature")
    //    aggStream.print()

    // 3. 需要输出当前最小的温度值, 以及最近的时间戳, 要用reduce
    val resultStream = dataStream.keyBy(_.id).reduce((curState, newData) => {
      SensorReading(curState.id, newData.timestamp, curState.temperature.min(newData.temperature))
    })
    //    resultStream.print()

    val resultStream1 = dataStream.keyBy(_.id).reduce(new ReduceFunction[SensorReading] {
      override def reduce(t: SensorReading, t1: SensorReading): SensorReading = {
        SensorReading(t.id, t1.timestamp, t.temperature.min(t1.temperature))
      }
    })
    //    resultStream1.print()

    // 4. 多流转换操作
    // 4.1 分流，将传感器温度数据分成低温、高温两条流
    val splitStream = dataStream.split(data => {
      if (data.temperature > 30) {
        Seq("high")
      } else {
        Seq("low")
      }
    })
    val highTempStream = splitStream.select("high")
    val lowTempStream = splitStream.select("low")
    val allTempStream = splitStream.select("high", "low")

    //    highTempStream.print("high")
    //    lowTempStream.print("low")
    //    allTempStream.print("all")

    // 4.2 合流, connect
    val warningStream = highTempStream.map(data => (data.id, data.temperature))
    val connectedStream: ConnectedStreams[(String, Double), SensorReading] = warningStream.connect(lowTempStream)

    // 用coMap对数据进行分别处理
    val coMapResultStream = connectedStream.map(warningData => {
      (warningData._1, warningData._2, "warning")
    }, lowTempData => {
      (lowTempData.id, "healthy")
    })
//    coMapResultStream.print("coMap")

    // 4.3 union 合流
    val unionStream = highTempStream.union(lowTempStream)
//    unionStream.print("unionStream")

    // 5. 自定义过滤器
    dataStream.filter(new MyFilter).print("filter")

    environment.execute(this.getClass.getSimpleName)
  }
}

class MyFilter extends FilterFunction[SensorReading]{
  override def filter(t: SensorReading): Boolean = {
    t.id.startsWith("sensor_1")
  }
}

/**
 * 富函数, 可以获取到运行时上下文, 还有一些生命周期
 */
class MyRichMapper extends RichMapFunction[SensorReading, String]{
  override def map(in: SensorReading): String = {
    in.id + " temperature"
  }
}