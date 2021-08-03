package com.bigdata.apitest

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object TransformTest {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val inputPath = "src/main/resources/sensor.txt"
    val stream = environment.readTextFile(inputPath)

    // 先转换成样例类类型
    val dataStream = stream.map(data => {
      val arr = data.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    // 分组聚合, 输出每个传感器当前最小值
    val aggStream = dataStream.keyBy(_.id).minBy("temperature")
//    aggStream.print()

    // 需要输出当前最小的温度值, 以及最近的时间戳, 要用reduce
    val resultStream = dataStream.keyBy(_.id).reduce((curState, newData) => {
      SensorReading(curState.id, newData.timestamp, curState.temperature.min(newData.temperature))
    })
//    resultStream.print()

    val resultStream1 = dataStream.keyBy(_.id).reduce(new ReduceFunction[SensorReading] {
      override def reduce(t: SensorReading, t1: SensorReading): SensorReading = {
        SensorReading(t.id, t1.timestamp, t.temperature.min(t1.temperature))
      }
    })
    resultStream1.print()

    environment.execute(this.getClass.getSimpleName)
  }
}
