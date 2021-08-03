package com.bigdata

import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random


/**
 * 自定义source function
 */
class SensorSource() extends SourceFunction[SensorReading] {

  // 定义一个标识位，用来表示数据源是否正常发出数据
  val running: Boolean = true

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    val random = new Random()

    var curTemp = 1.to(10).map(i => ("sensor_" + i, random.nextDouble() * 50))

    while (running) {
      curTemp = curTemp.map(data=>
        (data._1, data._2 + random.nextGaussian())
      )
      val currentTime = System.currentTimeMillis()
      curTemp.foreach(
        data=>sourceContext.collect(SensorReading(data._1, currentTime, data._2))
      )
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {

  }
}
