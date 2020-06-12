package com.later.flink_demo

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * @description: ${description}
  * @author: Liu Jun Jun
  * @create: 2020-05-29 17:53
  **/
object flink2Hbase {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.streaming.api.scala._

    val stuDS: DataStream[student] = env
        .socketTextStream("bigdata102",3456)
      .map(s => {
        val stu: Array[String] = s.split(",")
        student(stu(0),stu(1).toInt)
      })
    //val resultDS: DataStream[student] = fileDS.keyBy(0).sum(1)

    val hBaseSink: HBaseSink = new HBaseSink("WordCount","info1")

    stuDS.addSink(hBaseSink)

    env.execute("app")
  }
}


