package com.bigdata.sideoutputtest

import com.bigdata.apitest.SensorReading
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SideOutPutTest {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    val inputStream = environment.socketTextStream("dev41", 7777)

    val dataStream = inputStream.map(data => {
      val arr = data.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    val highTempStream = dataStream.process(new SplitTempProcessor(30.0))

    highTempStream.print("high")
    highTempStream.getSideOutput(new OutputTag[(String, Long, Double)]("low")).print("low")

    environment.execute()
  }
}

class SplitTempProcessor(threshold: Double) extends ProcessFunction[SensorReading, SensorReading] {
  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    if (value.temperature > threshold){
      // 如果当前温度值大于阈值, 输出到主流
      out.collect(value)
    } else {
      // 如果不超过阈值, 输出到测输出流
      ctx.output(new OutputTag[(String, Long, Double)]("low"),(value.id, value.timestamp, value.temperature))
    }
  }
}
