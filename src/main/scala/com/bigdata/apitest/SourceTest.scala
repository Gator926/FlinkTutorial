package com.bigdata.apitest

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import java.util.Properties
import scala.util.Random

// 定义样例类, 温度传感器
case class SensorReading(id: String, timestamp: Long, temperature: Double)

object SourceTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 1. 从集合中读取数据
//    val dataList = List(
//      SensorReading("sensor_1", 1547718199, 35.8),
//      SensorReading("sensor_6", 1547718201, 15.4),
//      SensorReading("sensor_7", 1547718202, 6.7),
//      SensorReading("sensor_10", 1547718205, 38.1)
//    )
//
//    env.fromCollection(dataList).print()

    // 2. 从文件中读取数据
//    val inputPath = "src/main/resources/sensor.txt"
//    env.readTextFile(inputPath).print()

    // 3. 从kafka中读取数据
//    val properties = new Properties
//    properties.setProperty("bootstrap.servers", "localhost:9092")
//    val stream3 = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))
//    stream3.print()

    // 4. 自动以Source
    val stream4 = env.addSource(new SensorSource())
    stream4.print()

    env.execute("source test")
  }
}

class SensorSource() extends SourceFunction[SensorReading]{
  // 定义一个标志位, 用来表示数据源是否正常发出数据
  var running: Boolean = true

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    // 定义一个随机数发生器
    val random = new Random()

    var curTemp = 1.to(10).map(index => {
      (s"sensor_$index", random.nextDouble() * 100)
    })

    // 定义无限循环, 不停产生数据, 除非被cancel
    while (running){
      // 在上次数据基础上微调, 更新温度值
      curTemp = curTemp.map(data=>
        (data._1, data._2 + random.nextGaussian())
      )

      // 获取当前时间戳, 加入到数据中
      val curTime = System.currentTimeMillis()
      curTemp.foreach(
        data=>sourceContext.collect(SensorReading(data._1, curTime, data._2))
      )
      Thread.sleep(100)
    }
  }

  override def cancel(): Unit = running = false
}