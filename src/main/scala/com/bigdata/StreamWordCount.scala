package com.bigdata

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    // 从外部命令中提取参数, 作为socket主机名和端口号
    val paramTool: ParameterTool = ParameterTool.fromArgs(args)

    // 创建流处理执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    val inputDataStream = env.socketTextStream(paramTool.get("host"), paramTool.getInt("port"))

    inputDataStream.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)
      .print().setParallelism(1)

    // 启动任务执行
    env.execute("stream work count")
  }
}
