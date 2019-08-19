package com.atguigu.gmall0311.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0311.common.constants.GmallConstants
import com.atguigu.gmall0311.realtime.bean.StartupLog
import com.atguigu.gmall0311.realtime.util.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.phoenix.spark._
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object DauApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)
    //    inputDstream.foreachRDD{
    //      rdd => {
    //        println(rdd.map(_.value()).collect().mkString("\n"))
    //      }
    //    }

    val startupLogDstream: DStream[StartupLog] = inputDstream.map { record =>
      val startupJsonString: String = record.value()
      val startupLog: StartupLog = JSON.parseObject(startupJsonString, classOf[StartupLog])
      val datetimeString: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(startupLog.ts))
      val dateArr: Array[String] = datetimeString.split(" ")
      startupLog.logDate = dateArr(0)
      startupLog.logHour = dateArr(1)

      startupLog
    }


    val filteredDstream: DStream[StartupLog] = startupLogDstream.transform { rdd =>
      //  driver
      // 利用清单进行过滤 去重
      println("过滤前：" + rdd.count())
      val jedis: Jedis = new Jedis("hadoop102", 6379) //driver 每个执行周期查询redis获得清单 通过广播变量发送到executor中
    val dauKey = "dau:" + new SimpleDateFormat("yyyy-MM-dd").format(new Date());
      val dauSet: util.Set[String] = jedis.smembers(dauKey)
      val dauBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauSet)
      jedis.close()
      val filteredRDD: RDD[StartupLog] = rdd.filter { startupLog => //executor
        !dauBC.value.contains(startupLog.mid)
      }


      println("过滤后： " + filteredRDD.count())
      filteredRDD

    }

        val groupByMidRDD: DStream[(String, Iterable[StartupLog])] = filteredDstream.map(startuplog => (startuplog.mid,startuplog)).groupByKey()
        val realFilteredDstream: DStream[StartupLog] = groupByMidRDD.flatMap {
          case (mid, startupLog) => {
            startupLog.take(1)
          }
        }
    realFilteredDstream.cache()

    realFilteredDstream.foreachRDD {
      rdd =>
        rdd.foreachPartition { startuplogIter => {
          val jedis: Jedis = new Jedis("hadoop102", 6379)
          for (startuplog <- startuplogIter) {
            val dauKey = "dau:" + startuplog.logDate
            //            println(dauKey+"::::"+startuplog.mid)
            jedis.sadd(dauKey, startuplog.mid)
          }
          jedis.close()
        }
        }


    }
    realFilteredDstream.cache()
    realFilteredDstream.foreachRDD{rdd=>

      rdd.saveToPhoenix("GMALL0311_DAU",Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        new Configuration,Some("hadoop102,hadoop103,hadoop104:2181"))

    }

    println("启动流程")
    ssc.start()
    ssc.awaitTermination()

  }
}
