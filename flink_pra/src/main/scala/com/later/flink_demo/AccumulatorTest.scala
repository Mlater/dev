package com.later.flink_demo

import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * @description: ${description}
  * @author: Liu Jun Jun
  * @create: 2020-06-12 17:55
  **/
object AccumulatorTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.streaming.api.scala._

    val dataDS = env
        .readTextFile("input/word.txt")
      //.socketTextStream("bigdata101", 3456)


    val resultDS: DataStream[String] = dataDS.map(new RichMapFunction[String, String] {

      //第一步：定义累加器//第一步：定义累加器
      private val numLines = new IntCounter

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        //注册累加器
        getRuntimeContext.addAccumulator("num-lines", this.numLines)
      }

      override def map(value: String): String = {
        this.numLines.add(1)
        numLines
        value
      }

      override def close(): Unit = super.close()
    })


    resultDS.print("单词输入个数统计：")

    val jobExecutionResult = env.execute("单词统计")
    println(jobExecutionResult.getAccumulatorResult("num-lines"))

  }

}
