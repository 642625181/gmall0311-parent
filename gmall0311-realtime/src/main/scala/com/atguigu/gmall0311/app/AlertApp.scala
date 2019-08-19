package com.atguigu.gmall0311.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0311.bean.{AlertInfo, EventInfo}
import com.atguigu.gmall0311.common.constants.GmallConstants
import com.atguigu.gmall0311.realtime.util.MyKafkaUtil
import com.atguigu.gmall0311.util.MyEsUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.control.Breaks

object AlertApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("AlertApp").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)
    val eventInfoDstream: DStream[EventInfo] = inputDStream.map { record =>
      val enentInfo: EventInfo = JSON.parseObject(record.value(), classOf[EventInfo])
      enentInfo
    }
    eventInfoDstream.cache()

    val eventWindosDStream: DStream[EventInfo] = eventInfoDstream.window(Seconds(300), Seconds(5))
    val groupbyMidDStream: DStream[(String, Iterable[EventInfo])] = eventWindosDStream.map(event => (event.mid, event)).groupByKey()
    val checkedDStream: DStream[(Boolean, AlertInfo)] = groupbyMidDStream.map {
      case (mid, eventItr) => {
        val couponUidSet = new util.HashSet[String]()
        val itemsSet = new util.HashSet[String]()
        val eventList = new util.ArrayList[String]()
        var hasClickItem = false
        Breaks.breakable(
          for (eventInfo: EventInfo <- eventItr) {
            eventList.add(eventInfo.evid)
            if (eventInfo.evid == "coupon") {
              couponUidSet.add(eventInfo.uid)
              itemsSet.add(eventInfo.itemid)
            }
            if (eventInfo.evid == "clickItem") {
              hasClickItem = true
              Breaks
            }
          }
        )
        (couponUidSet.size() >= 3 && !hasClickItem, AlertInfo(mid, couponUidSet, itemsSet, eventList, System.currentTimeMillis()))
      }
    }
    val alertDStream: DStream[AlertInfo] = checkedDStream.filter(_._1).map(_._2)

    //    checkedDStream.foreachRDD{
    //      rdd=>{
    //        println(rdd.collect().mkString("\n"))
    //      }
    //    }

    alertDStream.foreachRDD { rdd =>
      rdd.foreachPartition { alertItr => {
        val list: List[AlertInfo] = alertItr.toList
        val alertListWithId: List[(String, AlertInfo)] = list.map(alertInfo => (alertInfo.mid + "_" + alertInfo.ts / 1000 / 60, alertInfo))

        MyEsUtil.indexBulk(GmallConstants.ES_INDEX_ALERT, alertListWithId)
      }

      }

    }

    ssc.start()
    ssc.awaitTermination()
  }

}
