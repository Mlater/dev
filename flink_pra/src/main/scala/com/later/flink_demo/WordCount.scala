package com.later.flink_demo

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

/**
  * @description: ${description}
  * @author: Liu Jun Jun
  * @create: 2020-06-04 10:10
  **/
object WordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val wordDS: DataStream[String] = env.socketTextStream("bigdata101",3456)

    val resultDS = wordDS.map((_,1)).keyBy(_._1).sum(1)

    resultDS.print("ceshi:")

    env.execute("wordCount")
  }

}
