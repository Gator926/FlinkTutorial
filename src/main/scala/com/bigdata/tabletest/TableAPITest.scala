package com.bigdata.tabletest

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment

object TableAPITest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)

    val settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val oldStreamTableEnv = StreamTableEnvironment.create(env, settings)

    env.execute()
  }
}
