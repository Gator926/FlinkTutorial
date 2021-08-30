package com.bigdata.tabletest

import com.bigdata.apitest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.types.Row

object AggregateFunctionTest {
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

    /**
     * table api
     */
    val temp = new AvgTemp()
    sensorTable
      .groupBy('id)
      .aggregate(temp('temperature) as 'avgTemp)
      .select('id, 'avgTemp)
//      .toRetractStream[Row].print()

    /**
     * sql
     */
    tableEnv.createTemporaryView("sensor", sensorTable)
    tableEnv.registerFunction("avgTemp", temp)
    tableEnv.sqlQuery(
      """
        |select id, avgTemp(temperature) from sensor group by id
        |""".stripMargin)
      .toRetractStream[Row].print()

    env.execute()
  }
}

/**
 * 定义一个类，专门表示聚合的状态
 */
class AvgTemAcc {
  var sum: Double = 0.0
  var count: Int = 0
}

/**
 * 自定义聚合函数, 求每个传感器的平均温度值, 保存状态(tempSum, tempCount)
 */
class AvgTemp extends AggregateFunction[Double, AvgTemAcc]{
  override def getValue(accumulator: AvgTemAcc): Double = {
    accumulator.sum / accumulator.count
  }

  override def createAccumulator(): AvgTemAcc = new AvgTemAcc

  /**
   * 还要实现一个具体的处理计算函数, accumulate
   */
  def accumulate(accumulator: AvgTemAcc, temp: Double): Unit ={
    accumulator.sum += temp
    accumulator.count += 1
  }
}