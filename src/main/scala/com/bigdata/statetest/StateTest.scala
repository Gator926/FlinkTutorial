package com.bigdata.statetest

import com.bigdata.apitest.SensorReading
import org.apache.flink.api.common.functions.{ReduceFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ReducingState, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

import java.util

object StateTest {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    val inputStream = environment.socketTextStream("dev41", 7777)

    val dataStream = inputStream.map(data => {
      val arr = data.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    environment.execute()
  }
}

/**
 * keyed state测试: 必须定义在RichFunction中，因为需要运行时上下文
 */
class myRichMapper extends RichMapFunction[SensorReading, String]{

  var valueState: ValueState[Double] = _
  lazy val listState: ListState[Int] = getRuntimeContext.getListState(new ListStateDescriptor[Int]("liststate", classOf[Int]))
  lazy val mapState: MapState[String, Double] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Double]("mapstate", classOf[String], classOf[Double]))
  lazy val reduceState: ReducingState[SensorReading] = getRuntimeContext.getReducingState(new ReducingStateDescriptor[SensorReading]("reducing", new MyStateReducer, classOf[SensorReading]))

  override def open(parameters: Configuration): Unit = {
    valueState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("valuestate", classOf[Double]))
  }

  override def map(in: SensorReading): String = {
    // 状态的读写
    // valueState
    val myV = valueState.value()
    // listState
    listState.add(1)
    val list = new util.ArrayList[Int]()
    list.add(2)
    list.add(3)
    listState.addAll(list)
    valueState.update(in.temperature)
    listState.update(list)
    // mapState
    mapState.contains("sensor_1")
    mapState.get("sensor_1")
    mapState.put("sensor_1", 1)
    // reduceState
//    reduceState.add()
//    reduceState.get()

    in.id
  }

}


class MyStateReducer extends ReduceFunction[SensorReading]{
  override def reduce(value1: SensorReading, value2: SensorReading): SensorReading = {
    SensorReading(value2.id, value2.timestamp, value2.temperature.min(value1.temperature))
  }
}