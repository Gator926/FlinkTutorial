package com.bigdata.tabletest

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table, TableEnvironment}
import org.apache.flink.table.api.scala.{BatchTableEnvironment, StreamTableEnvironment}
import org.apache.flink.table.descriptors.{Csv, FileSystem, OldCsv, Schema}
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

    // 2. 连接外部系统, 读取数据, 注册表
    val filePath = "src/main/resources/sensor.txt"
    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new Csv)
      .withSchema(
        new Schema()
          .field("id", DataTypes.STRING())
          .field("timestamp", DataTypes.BIGINT())
          .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputTable")

    val inputTable: Table = tableEnv.from("inputTable")
    inputTable.toAppendStream[(String, Long, Double)].print("inputTable")



    env.execute()
  }
}
