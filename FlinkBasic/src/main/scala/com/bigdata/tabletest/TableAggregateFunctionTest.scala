package com.bigdata.tabletest

import com.bigdata.apitest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

object TableAggregateFunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    // 读取数据
    val inputPath = "FlinkBasic/src/main/resources/sensor.txt"
    val inputStream = env.readTextFile(inputPath)

    // 先转换成样例类类型（简单转换操作）
    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
      })

    val sensorTable = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp.rowtime as 'ts)

    // table api
    val top2Temp = new Top2Temp()
    sensorTable
      .groupBy('id)
      .flatAggregate(top2Temp('temperature) as ('temp, 'rank))
      .select('id, 'temp, 'rank)
      .toRetractStream[Row]
//      .print("table api")

    // sql
    tableEnv.createTemporaryView("sensor", sensorTable)
    tableEnv.registerFunction("top2Temp", top2Temp)
    tableEnv.sqlQuery(
      """
        |select id, top2Temp(temperature) as temp(temperature, rank) from sensor group by id
        |""".stripMargin)
      .toRetractStream[Row].print()

    env.execute()
  }
}

// 定义一个类, 用来表示表聚合函数的状态
class Top2TempAcc{
  var highestTemp: Double = Double.MinValue
  var secondHighestTemp: Double = Double.MinValue
}

// 自定义表聚合函数, 提取所有温度值中最高的两个温度，输出(tmp, rank)
class Top2Temp extends TableAggregateFunction[(Double, Int), Top2TempAcc]{
  override def createAccumulator(): Top2TempAcc = new Top2TempAcc

  // 实现计算聚合结果的函数
  def accumulate(acc: Top2TempAcc, temp: Double): Unit ={
    // 判断当前温度值, 是否比状态中的值打
    if(temp > acc.highestTemp){
      // 如果比最高温度还高，排在第一，原来的第一顺位到第二位
      acc.secondHighestTemp = acc.highestTemp
      acc.highestTemp = temp
    } else if (temp < acc.highestTemp && temp > acc.secondHighestTemp){
      acc.secondHighestTemp = temp
    }
  }

  // 实现一个输出结果的方法，最终处理完表中所有数据时调用
  def emitValue(acc: Top2TempAcc, out: Collector[(Double, Int)]): Unit ={
    out.collect((acc.highestTemp, 1))
    out.collect((acc.secondHighestTemp, 2))
  }
}