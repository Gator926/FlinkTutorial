package com.bigdata.sinktest

import com.bigdata.apitest.SensorReading
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisSinkTest {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val inputPath = "src/main/resources/sensor.txt"
    val stream = environment.readTextFile(inputPath)

    val dataStream: DataStream[SensorReading] = stream.map(data => {
      val arr = data.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    // 定义FlinkJedisConfigBase
    val config = new FlinkJedisPoolConfig.Builder()
      .setHost("dev41")
      .setPort(6379)
      .build()

    val redisSink: RedisSink[SensorReading] = new RedisSink[SensorReading](config, new MyRedisMapper)

    dataStream.addSink(redisSink)

    environment.execute(this.getClass.getSimpleName)
  }
}

class MyRedisMapper extends RedisMapper[SensorReading] {
  // 定义保存数据写入redis的命令，HSET 表名 key value
  override def getCommandDescription: RedisCommandDescription =
    new RedisCommandDescription(RedisCommand.HSET, "sensor_temp")

  // 将id指定为key
  override def getKeyFromData(data: SensorReading): String = data.id

  // 将温度指定为value
  override def getValueFromData(data: SensorReading): String = data.temperature.toString
}
