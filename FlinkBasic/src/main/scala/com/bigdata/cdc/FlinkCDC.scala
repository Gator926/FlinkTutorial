//package com.bigdata.cdc
//
//import com.ververica.cdc.connectors.mysql.MySqlSource
//import com.ververica.cdc.connectors.mysql.table.StartupOptions
//import com.ververica.cdc.debezium.{DebeziumSourceFunction, StringDebeziumDeserializationSchema}
//import org.apache.flink.streaming.api.scala._
//
//object FlinkCDC {
//  def main(args: Array[String]): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)
//
//    // 通过flink cdc构建sourceFunction
//    val sourceFunction = MySqlSource.builder[String]()
//      .hostname("localhost")
//      .port(3306)
//      .username("root")
//      .password("root")
//      .databaseList("cdc_test")
//      .tableList("cdc_test.user_info")
//      .deserializer(new StringDebeziumDeserializationSchema)
//      .startupOptions(StartupOptions.initial())
//      .build()
//
//    val dataStreamSource = env.addSource(sourceFunction)
//
//    // 数据打印
//    dataStreamSource.print()
//
//    env.execute()
//  }
//}
