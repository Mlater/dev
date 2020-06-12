package com.later.flink_demo

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

/**
  * @description: ${description}
  * @author: Liu Jun Jun
  * @create: 2020-06-01 11:44
  **/
object flink2ES {
  def main(args: Array[String]): Unit = {
    // 转换
    val env: StreamExecutionEnvironment =
      StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    // 对数据流的处理可以采用Scala API，也可以采用Java API
    //val dataDS: DataStream[String] = env
        //.readTextFile("D:\\dianjue\\tms_2.0_big_data\\djBigData\\flink_realtime\\input\\word.txt")
      //.socketTextStream("bigdata102", 3456)
    /*.fromCollection(
    List("ha", "fsa", "gf","af")
  )*/
    val topic = "ctm_student"
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

    /*val dataDS = kafkaDS.map(
      line => {
        val value = JSON.parse(line)

        //WaterSensor( datas(0), datas(1).toLong, datas(2).toInt )
      }
    )*/


    val httpHosts = new java.util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("bigdata101", 9200))

    /*val ds: DataStream[WaterSensor] = dataDS.map(
      s => {
        WaterSensor(s, 1L, 1)
      }
    )*/

    //ds.print("kafka>>>>")

    val esSinkBuilder: ElasticsearchSink.Builder[String] = new ElasticsearchSink.Builder[String](
    httpHosts, new ElasticsearchSinkFunction[String] {
        def createIndexRequest(element: String): IndexRequest = {
          val json = new java.util.HashMap[String, String]
          json.put("data", element)

          return Requests.indexRequest()
            .index("ws")
            .`type`("readingData")
            .source(json)
        }
     override def process(t: String, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {

       requestIndexer.add(createIndexRequest(t))
       println("saved successfully")
      }
    })



    esSinkBuilder.setBulkFlushMaxActions(1)
    // TODO 将数据发送到ES中
    kafkaDS.addSink(esSinkBuilder.build())

    env.execute()
  }
}
