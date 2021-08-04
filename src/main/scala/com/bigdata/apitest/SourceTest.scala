package com.bigdata.apitest

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import java.util.Properties

// 定义样例类, 温度传感器
case class SensorReading(id: String, timestamp: Long, temperature: Double)

object SourceTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 1. 从集合读取数据
    val dataList = List(
      SensorReading("sensor_1", 1547718199, 35.8),
      SensorReading("sensor_6", 1547718201, 15.4),
      SensorReading("sensor_7", 1547718202, 6.7),
      SensorReading("sensor_10", 1547718205, 38.1)
    )
//    val stream1 = env.fromCollection(dataList)

    // 2. 从文件读取数据
    val inputPath = "src/main/resources/sensor.txt"
//    val stream1 = env.readTextFile(inputPath)

    // 3. 从kafka中读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "dev41:9092,dev42:9092,dev43:9092")
//    val stream1 = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))

    // 4. 自定义source
    val stream1 = env.addSource(new SensorSource())
    stream1.print()

    env.execute(this.getClass.getName)
  }
}
