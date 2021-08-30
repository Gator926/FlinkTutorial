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
    environment.getConfig.setAutoWatermarkInterval(50)

    val inputStream = environment.socketTextStream("dev41", 7777)

    val dataStream = inputStream.map(data => {
      val arr = data.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(3)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
      })

    dataStream.print("data")

    val latetag = new OutputTag[(String, Double, Long)]("late")

    val resultStream = dataStream.map(data => SensorReading(data.id, data.timestamp, data.temperature))
      .keyBy(_.id)
      .timeWindow(Time.seconds(15))
      .allowedLateness(Time.minutes(1))
      .reduce((curRes, newData) => SensorReading(curRes.id, newData.timestamp, curRes.temperature.min(newData.temperature)))

    resultStream.getSideOutput(latetag).print("late")
    resultStream.print("result")

    environment.execute()
  }
}
