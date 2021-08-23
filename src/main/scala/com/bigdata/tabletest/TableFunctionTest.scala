package com.bigdata.tabletest

import com.bigdata.apitest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

object TableFunctionTest {
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
    val inputPath = "src/main/resources/sensor.txt"
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
    val split = new Split("_")
    sensorTable
      .joinLateral(split('id) as ('word, 'length))
      .select('id, 'word, 'length)
      .toAppendStream[Row]
      .print()

    /**
     * sql
     */
    tableEnv.createTemporaryView("sensor", sensorTable)
    tableEnv.registerFunction("split", split)
    tableEnv.sqlQuery(
      """
        |select id, word, length from sensor, lateral table(split(id)) as splitid(word, length)
        |""".stripMargin)
      .toAppendStream[Row].print("sql")

    env.execute()
  }
}

/**
 * 自定义TableFunction
 */
class Split(separator: String) extends TableFunction[(String, Int)]{
  def eval(str: String): Unit ={
    str.split(separator).foreach(word=>{
      collect((word, word.length))
    })
  }
}