package com.bigdata.tabletest

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.api.scala.{BatchTableEnvironment, StreamTableEnvironment}

object TableAPITest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)

    // 1.1 基于老版本planner的流处理
    val settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val oldStreamTableEnv = StreamTableEnvironment.create(env, settings)

    // 1.2 基于老版本的批处理
    val batchEnv = ExecutionEnvironment.getExecutionEnvironment
    val oldBatchTableEnv = BatchTableEnvironment.create(batchEnv)

    // 1.3 基于blink planner的流处理
    val blinkStreamSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val blinkStreamTableEnv = StreamTableEnvironment.create(env, blinkStreamSettings)

    // 1.4 基于blink planner的批处理
    val blinkBatchSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
    val blinkBatchTableEnv = TableEnvironment.create(blinkBatchSettings)

    env.execute()
  }
}
