package com.later.flink_demo

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.walkthrough.common.entity.Alert
import org.apache.flink.walkthrough.common.entity.Transaction
/**
  * @description:
  * @author: Liu Jun Jun
  * @create: 2020-06-03 11:24
  **/
object FraudDetector {
  val SMALL_AMOUNT: Double = 1.00
  val LARGE_AMOUNT: Double = 500.00
  val ONE_MINUTE: Long     = 60 * 1000L
}

@SerialVersionUID(1L)
class FraudDetector extends KeyedProcessFunction[Long, Transaction, Alert] {

  @throws[Exception]
  def processElement(
                      transaction: Transaction,
                      context: KeyedProcessFunction[Long, Transaction, Alert]#Context,
                      collector: Collector[Alert]): Unit = {

    val alert = new Alert
    alert.setId(transaction.getAccountId)

    collector.collect(alert)
  }
}
