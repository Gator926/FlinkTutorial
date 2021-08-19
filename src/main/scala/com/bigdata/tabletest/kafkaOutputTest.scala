package com.bigdata.tabletest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Kafka, Schema}

object kafkaOutputTest {
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

    val filterTable = tableEnv.from("sensorTable")
      .select('id, 'temperature)
      .filter('id === "sensor_1")

    tableEnv.connect(
      new Kafka()
        .version("0.11")
        .topic("test")
        .property("zookeeper.connect", "dev41:2181")
        .property("bootstrap.servers", "dev41:9092")
    )
      .withFormat(new Csv)
      .withSchema(
        new Schema()
          .field("id", DataTypes.STRING())
          .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaOutputTable")

    filterTable.insertInto("kafkaOutputTable")

    env.execute()
  }
}
