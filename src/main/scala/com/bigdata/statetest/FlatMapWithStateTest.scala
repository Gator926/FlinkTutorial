package com.bigdata.statetest

import com.bigdata.apitest.SensorReading
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object FlatMapWithStateTest {
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
      .flatMapWithState[(String, Double, Double), Double]({
        case (data: SensorReading, None) => (List.empty, Some(data.temperature))
        case (data: SensorReading, lastTemp: Some[Double]) => {
          val diff = (data.temperature - lastTemp.get).abs
          if (diff > 10.0){
            ( List((data.id, lastTemp.get, data.temperature)), Some(data.temperature))
          } else {
            ( List.empty, Some(data.temperature))
          }
        }
      })

    alertStream.print("alert")
    
    environment.execute()
  }

}
