package com.bigdata

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object WorkCount {
  def main(args: Array[String]): Unit = {
    // 创建一个批处理的执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 从文件中读取数据
    val inputPath = "src/main/resources/hello.txt"
    val inputDataSet = env.readTextFile(inputPath)

    // 对数据进行转换处理统计，分限次，再按照word进行分组，最后进行聚合统计
    val resultDataSet = inputDataSet.flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0) // 以第一个元素作为key进行分组
      .sum(1) // 对所有分组的第二个元素求和

    resultDataSet.print()
  }
}
