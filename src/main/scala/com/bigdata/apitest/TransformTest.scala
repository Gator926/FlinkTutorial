package com.bigdata.apitest

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
    val aggStream = dataStream.keyBy(_.id).minBy("temperature")
    aggStream.print()

    environment.execute(this.getClass.getSimpleName)
  }
}
