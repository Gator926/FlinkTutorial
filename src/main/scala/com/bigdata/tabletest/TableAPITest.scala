package com.bigdata.tabletest

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table, TableEnvironment}
import org.apache.flink.table.api.scala.{BatchTableEnvironment, StreamTableEnvironment}
import org.apache.flink.table.descriptors.{Csv, FileSystem, Kafka, OldCsv, Schema}
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.scala._

object TableAPITest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)

    // 1.1 基于老版本planner的流处理
    val settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val oldStreamTableEnv = StreamTableEnvironment.create(env, settings)

    // 1.2 基于老版本的批处理
//    val batchEnv = ExecutionEnvironment.getExecutionEnvironment
//    val oldBatchTableEnv = BatchTableEnvironment.create(batchEnv)

    // 1.3 基于blink planner的流处理
//    val blinkStreamSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
//    val blinkStreamTableEnv = StreamTableEnvironment.create(env, blinkStreamSettings)

    // 1.4 基于blink planner的批处理
//    val blinkBatchSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
//    val blinkBatchTableEnv = TableEnvironment.create(blinkBatchSettings)

    // 2.1 连接外部系统, 读取数据, 注册表
    val filePath = "src/main/resources/sensor.txt"

    val schema: Schema = new Schema()
      .field("id", DataTypes.STRING())
      .field("timestamp", DataTypes.BIGINT())
      .field("temperature", DataTypes.DOUBLE())

    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new Csv)
      .withSchema(
        schema
      )
      .createTemporaryTable("inputTable")
//    val inputTable: Table = tableEnv.from("inputTable")
//    inputTable.toAppendStream[(String, Long, Double)].print("inputTable")

    // 2.2 从kafka读取数据
    tableEnv.connect(new Kafka()
    .version("0.11")
    .topic("sensor")
    .property("zookeeper.connect", "dev41:2181,dev42:2181,dev43:2181")
    .property("bootstrap.servers", "dev41:9092,dev42:9092,dev43:9092")
    )
      .withFormat(new Csv)
      .withSchema(
        schema
      ).createTemporaryTable("kafkaInputTable")
//      val kafkaInputTable: Table = tableEnv.from("kafkaInputTable")
//    kafkaInputTable.toAppendStream[(String, Long, Double)].print("kafkaInputTable")

    // 3. 查询转换
    // 3.1 table API
    val sensorTable = tableEnv.from("inputTable")
    sensorTable.select('id, 'temperature)
      .filter('id === "sensor_1")
      .toAppendStream[(String, Double)]
      .print("api")

    tableEnv.sqlQuery(
      """
        |select id, temperature
        |from inputTable
        |where id = 'sensor_1'
        |""".stripMargin
    ).toAppendStream[(String, Double)].print("sql")

    env.execute()
  }
}
