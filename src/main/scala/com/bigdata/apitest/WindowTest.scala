package com.bigdata.apitest

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTest {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    // 时间语义
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputPath = "src/main/resources/sensor.txt"
    val stream = environment.readTextFile(inputPath)

    val dataStream = stream.map(data => {
      val arr = data.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })
//      .assignAscendingTimestamps(_.timestamp * 1000L)   // 升序数据提取时间戳
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(3)) {
        override def extractTimestamp(element: SensorReading): Long = {
          element.timestamp * 1000L
        }
      })

    environment.execute()
  }
}
