package com.later.flink_demo

import java.util.Properties

import com.alibaba.fastjson.JSON
import com.later.flink_demo
import com.later.utils.ParseJsonData
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.util.JSONPObject

/**
  * @description: ${kafka Source测试}
  * @author: Liu Jun Jun
  * @create: 2020-06-10 10:56
  **/
object kafkaSource {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    //
    properties.setProperty("bootstrap.servers", "bigdata101:9092")
    properties.setProperty("group.id", "test")
    //设置序列化方式
    //properties.setProperty("key.deserializer", "JsonDeserializationSchema")
    //properties.setProperty("value.deserializer", "JSONKeyValueDeserializationSchema")
    //设置offset消费方式：可设置的参数为：earliest，latest，none
    properties.setProperty("auto.offset.reset", "latest")
    //earliest：当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
    //latest：当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
    //none：topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常

    val kafkaDS: DataStream[ObjectNode] = env.addSource(
      new FlinkKafkaConsumer011[ObjectNode]("test1",
        new JSONKeyValueDeserializationSchema(false),
        properties)
    )
    /*val kafkaDS: DataStream[String] = env.addSource(
      new FlinkKafkaConsumer011[String]("test1", new SimpleStringSchema(), properties)
    )
*/
    /*val StuDS: DataStream[Object] = kafkaDS.map(
      new MapFunction[String, Object]{
        override def map(value: String): Object = {
          val jSONObject = ParseJsonData.getJsonData(value)
          val tableName = jSONObject.get("table")
          val data: AnyRef = jSONObject.get("data")

          data.asInstanceOf[Stu]
          if (tableName.equals("a")){

            //data.asInstanceOf[Stu]
            JSON.parseObject(data.toString, classOf[Stu])
          }else{
            JSON.parseObject(data.toString, classOf[Stu1])
          }
        }
      }
    )*/
    val StuDS: DataStream[Stu] = kafkaDS.map(
      new MapFunction[ObjectNode, Stu]() {
        override def map(value: ObjectNode): Stu = {
          JSON.parseObject(value.get("value").toString, classOf[Stu])
        }
      }
    )

    new MapFunction[String,Int] {
      override def map(value: String): Int = {
        val lines = value.split(",")
        lines(1).toInt
      }
    }
    //kafkaDS.print("测试：")
    StuDS.print("测试：")

    env.execute("kafkaSource")
  }
}
