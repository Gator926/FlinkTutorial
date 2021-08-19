package com.bigdata.tabletest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors._

object MySQLTable {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val tableEnv = StreamTableEnvironment.create(env)

//    tableEnv.connect(new Kafka()
//      .version("0.11")
//      .topic("sensor")
//      .property("zookeeper.connect", "dev41:2181,dev42:2181,dev43:2181")
//      .property("bootstrap.servers", "dev41:9092,dev42:9092,dev43:9092")
//    )
//      .withFormat(new Csv)
//      .withSchema(
//        new Schema()
//          .field("id", DataTypes.STRING())
//          .field("timestamp", DataTypes.BIGINT())
//          .field("temperature", DataTypes.DOUBLE())
//      ).createTemporaryTable("sensorTable")

    val schema = new Schema()
      .field("id", DataTypes.STRING())
      .field("timestamps", DataTypes.BIGINT())
      .field("temperature", DataTypes.DOUBLE())

    tableEnv.connect(new FileSystem().path("src/main/resources/sensor.txt"))
      .withFormat(new Csv)
      .withSchema(schema)
      .createTemporaryTable("sensorTable")

    val aggTable = tableEnv.from("sensorTable")
      .groupBy('id)
      .select('id, 'temperature.avg as 'temp)

    /**
     * 一定要加?useSSL=false, 否则程序会报错
     */
    val sinkDDL =
      """
        |create table sensorTmp (
        |  id varchar(20) not null,
        |  temp double not null
        |) with (
        |  'connector.type' = 'jdbc',
        |  'connector.url' = 'jdbc:mysql://localhost:3306/flink?useSSL=false',
        |  'connector.table' = 'sensor',
        |  'connector.driver' = 'com.mysql.jdbc.Driver',
        |  'connector.username' = 'root',
        |  'connector.password' = 'root'
        |)
        |""".stripMargin
    tableEnv.sqlUpdate(sinkDDL)
    aggTable.toRetractStream[(String, Double)].print()

    aggTable.insertInto("sensorTmp")

    env.execute()
  }
}
