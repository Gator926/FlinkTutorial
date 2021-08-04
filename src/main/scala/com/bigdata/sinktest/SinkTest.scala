package com.bigdata.sinktest

import com.bigdata.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object SinkTest {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val inputPath = "src/main/resources/sensor.txt"
    val stream = environment.readTextFile(inputPath)

    val dataStream = stream.map(data => {
      val arr = data.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    dataStream.addSink(
      StreamingFileSink.forRowFormat(
        new Path("src/main/resources/out"),
        new SimpleStringEncoder[SensorReading]()
      ).build()
    )

    environment.execute(this.getClass.getSimpleName)
  }
}
