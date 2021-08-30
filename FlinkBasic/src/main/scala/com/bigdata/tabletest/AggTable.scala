package com.bigdata.tabletest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

object AggTable {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)

    val schema = new Schema()
      .field("id", DataTypes.STRING())
      .field("timestamps", DataTypes.BIGINT())
      .field("temperature", DataTypes.DOUBLE())

    tableEnv.connect(new FileSystem().path("FlinkBasic/src/main/resources/sensor.txt"))
      .withFormat(new Csv)
      .withSchema(schema)
      .createTemporaryTable("sensorTable")

    tableEnv.from("sensorTable")
      .groupBy('id)
      .select('id, 'temperature.avg as 'temperature)
      .toRetractStream[(String, Double)].print()

    env.execute()
  }
}
