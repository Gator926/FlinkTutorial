package com.bigdata.tabletest

import com.bigdata.apitest.SensorReading
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object Example {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    val inputPath = "src/main/resources/sensor.txt"
    val inputStream = environment.readTextFile(inputPath)

    val dataStream = inputStream.map(data => {
      val arr = data.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    dataStream.print()

    environment.execute()
  }
}
