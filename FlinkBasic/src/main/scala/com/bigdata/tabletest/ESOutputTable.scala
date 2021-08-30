package com.bigdata.tabletest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Elasticsearch, FileSystem, Json, Schema}

object ESOutputTable {
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

    val aggTable = tableEnv.from("sensorTable")
      .groupBy('id)
      .select('id, 'temperature.avg as 'temperature)

    tableEnv.connect(new Elasticsearch()
      .version("6")
      .host("dev41", 9200, "http")
      .index("sensor")
      .documentType("temperature")
    )
      .inUpsertMode()
      .withFormat(new Json())
      .withSchema(
        new Schema()
          .field("id", DataTypes.STRING())
          .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("esOutputTable")

    aggTable.insertInto("esOutputTable")

    env.execute()
  }
}
