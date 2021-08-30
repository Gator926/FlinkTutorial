package com.bigdata.sinktest

import com.bigdata.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

object KafkaSink {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val inputPath = "FlinkBasic/src/main/resources/sensor.txt"
    val stream = environment.readTextFile(inputPath)

    val dataStream = stream.map(data => {
      val arr = data.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble).toString
    })

    dataStream.addSink(new FlinkKafkaProducer011[String](
      "192.168.1.41:9092", "sensor_producer", new SimpleStringSchema())
    )

    environment.execute(this.getClass.getSimpleName)
  }
}
