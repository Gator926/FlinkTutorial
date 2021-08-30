package com.bigdata.tabletest

import com.bigdata.apitest.SensorReading
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._

object Example {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val inputPath = "FlinkBasic/src/main/resources/sensor.txt"
    val inputStream = environment.readTextFile(inputPath)

    val dataStream = inputStream.map(data => {
      val arr = data.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    // 创建表执行环境
    val tableEnv = StreamTableEnvironment.create(environment)

    // 基于流创建一张表
    val table = tableEnv.fromDataStream(dataStream)

    // api转换
    table.select("id, temperature")
      .filter("id == 'sensor_1'")
      .toAppendStream[(String, Double)]
      .print("result")

    // sql
    tableEnv.createTemporaryView("dataTable", table)
    tableEnv.sqlQuery("select id, temperature from dataTable where id = 'sensor_6'")
      .toAppendStream[(String, Double)]
      .print("sql")

    environment.execute()
  }
}
