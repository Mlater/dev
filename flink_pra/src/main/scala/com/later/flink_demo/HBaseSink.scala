package com.later.flink_demo

/**
  * @description: ${description}
  * @author: Liu Jun Jun
  * @create: 2020-06-01 14:41
  **/
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

class HBaseSink(tableName: String, family: String) extends RichSinkFunction[student] {

  var conn: Connection = _

  override def open(parameters: Configuration): Unit = {
    conn = HbaseFactoryUtil.getConn()
  }

  override def invoke(value: student): Unit = {
    val t: Table = conn.getTable(TableName.valueOf(tableName))

    val put: Put = new Put(Bytes.toBytes(value.age))
    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("name"), Bytes.toBytes(value.name))
    //put.addColumn(Bytes.toBytes(family), Bytes.toBytes("age"), Bytes.toBytes(value.id))
    t.put(put)
    t.close()
  }
  override def close(): Unit = {
    /*super.close()
    conn.close()*/
  }
}

