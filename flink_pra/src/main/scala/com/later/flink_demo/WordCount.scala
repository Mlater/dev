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

    //获取上下文环境
    val contextEnv = StreamExecutionEnvironment.getExecutionEnvironment
    //获取本地环境
    //val localEnv = StreamExecutionEnvironment.createLocalEnvironment(1)
    //获取集群环境
    //val romoteEnv = StreamExecutionEnvironment.createRemoteEnvironment("bigdata101",3456,2,"/ce.jar")

    val wordDS: DataStream[String] = contextEnv.socketTextStream("bigdata101",3456)

    //contextEnv.readTextFile("/word.txt")

    //val value: DataStream[String] = contextEnv.fromCollection(List("1","2"))

    val resultDS = wordDS.map((_,1)).keyBy(_._1).reduce{
      (s1,s2) =>{
        (s1._1,s2._2)
      }
    }
      //.min(1)
      //.sum(1)

    resultDS.print("ceshi:")

    contextEnv.execute("wordCount")


  }

}
