package com.bigdata.tabletest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

object FileOutputTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)
    env.setParallelism(1)

    val schema = new Schema()
      .field("id", DataTypes.STRING())
      .field("timestamps", DataTypes.BIGINT())
      .field("temperature", DataTypes.DOUBLE())

    tableEnv.connect(new FileSystem().path("src/main/resources/sensor.txt"))
      .withFormat(new Csv)
      .withSchema(schema)
      .createTemporaryTable("sensorTable")

    val sensorTable = tableEnv.from("sensorTable")

    val filterTable = sensorTable.select('id, 'temperature)
      .filter('id === "sensor_1")

    tableEnv.connect(new FileSystem().path("src/main/resources/sensor_output.txt"))
      .withSchema(new Schema()
      .field("id", DataTypes.STRING())
      .field("temperature", DataTypes.DOUBLE())
      )
      .withFormat(new Csv)
      .createTemporaryTable("outputTable")

    filterTable.insertInto("outputTable")

    env.execute()
  }
}
