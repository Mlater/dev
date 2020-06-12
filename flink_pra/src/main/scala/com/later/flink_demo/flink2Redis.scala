package com.later.flink_demo

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
  * @description: ${description}
  * @author: Liu Jun Jun
  * @create: 2020-06-12 11:23
  **/
object flink2Redis {

  def main(args: Array[String]): Unit = {
    // 转换
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    val topic = "test1"
    val properties = new java.util.Properties()
    properties.setProperty("bootstrap.servers", "bigdata101:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    // 从kafka中获取数据
    val kafkaDS: DataStream[String] =
      env.addSource(
        new FlinkKafkaConsumer011[String](
          topic,
          new SimpleStringSchema(),
          properties) )

    kafkaDS.print("data:")

    val conf = new FlinkJedisPoolConfig.Builder().setHost("bigdata103").setPort(6379).build()

    kafkaDS.addSink(new RedisSink[String](conf, new RedisMapper[String] {
      override def getCommandDescription: RedisCommandDescription = {
        new RedisCommandDescription(RedisCommand.HSET,"sensor")
      }

      override def getKeyFromData(t: String): String = {
        t.split(",")(0)
      }

      override def getValueFromData(t: String): String = {
        t.split(",")(1)
      }
    }))

    env.execute()
  }
}
