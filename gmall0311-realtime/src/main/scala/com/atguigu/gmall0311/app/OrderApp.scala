package com.atguigu.gmall0311.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0311.realtime.bean.OrderInfo
import com.atguigu.gmall0311.realtime.util.MyKafkaUtil
import javax.security.auth.login.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OrderApp {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("order_app").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(5))
    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("",ssc)
    val orderDstream: DStream[OrderInfo] = inputDstream.map { record =>
      val jsonstr: String = record.value()
      val orderInfo: OrderInfo = JSON.parseObject(jsonstr, classOf[OrderInfo])
      val datetimeArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = datetimeArr(0)
      val hourStr: String = datetimeArr(1).split(":")(0)
      orderInfo.create_hour = hourStr

      val tuple: (String, String) = orderInfo.consignee_tel.splitAt(4)
      orderInfo.consignee_tel = tuple._1 + "*******"

      orderInfo
    }
    orderDstream.foreachRDD{rdd =>
      new Configuration()

    }
  }
}
