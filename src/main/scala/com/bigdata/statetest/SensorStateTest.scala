package com.bigdata.statetest

import com.bigdata.apitest.SensorReading
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SensorStateTest {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    val inputStream = environment.socketTextStream("dev41", 7777)

    val dataStream = inputStream.map(data => {
      val arr = data.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    // 需求: 对于温度传感器温度值跳变超过10度报警
    val alertStream = dataStream
      .keyBy(_.id)
      .flatMap(new TempChangeAlert(10.0))

    alertStream.print("alert")
    
    environment.execute()
  }

}

/**
 * 实现自定义RichFlatMapFunction
 */
class TempChangeAlert(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {
  // 定义状态保存上一次的温度值
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("state", classOf[Double]))

  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    // 获取上一次的温度值
    val lastTemp = lastTempState.value()
    // 跟最新的温度值求差值做比较
    if (lastTemp != Double.NaN){
      val diff = (value.temperature - lastTemp).abs
      if (diff > threshold){
        out.collect((value.id, lastTemp, value.temperature))
      }
      // 更新状态
      lastTempState.update(value.temperature)
    }
  }
}
