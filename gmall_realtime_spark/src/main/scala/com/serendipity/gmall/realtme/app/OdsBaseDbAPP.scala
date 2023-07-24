package com.serendipity.gmall.realtme.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.serendipity.gmall.realtme.util.{MyKafkaUtil, MyOffsetsUtils, MyRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import java.util

/**
 *
 * 1.准备实时环境
 * 2.从redis中获取偏移量
 * 3.根据偏移量去Kafka中消费数据
 * 4.消费完数据
 * 5.数据处理过程
 * -> 转换数据结构
 * -> 分流
 * -> 事实数据写入Kafka
 * -> 纬度数据写入redis
 * 6.刷写到磁盘
 * 7.更新offset
 */
object OdsBaseDbAPP {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\soft\\hadoop")
    val conf: SparkConf = new SparkConf().setAppName("ods_base_db_app").setMaster("local[1]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    val topic: String = "ODS_BASE_DB"
    val groupId: String = "ODS_BASE_DB_GROUP"
    val offsetMap: Map[TopicPartition, Long] = MyOffsetsUtils.readOffset(topic, groupId)
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.nonEmpty) {
      kafkaDStream = MyKafkaUtil.getKafkaDStream(topic, ssc, groupId, offsetMap)
    } else {
      kafkaDStream = MyKafkaUtil.getKafkaDStream(topic, ssc, groupId)
    }

    //从Kafka中获取offset
    var offsetRanges: Array[OffsetRange] = null
    val offsetStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })
    //封装处理逻辑
    val jsonStream: DStream[JSONObject] = offsetStream.map(consumerRecord => {
      val str: String = consumerRecord.value()
      val jSONObject: JSONObject = JSON.parseObject(str)
      jSONObject
    })
//    //事实表清单
//    val factTables: Array[String] = Array[String]("order_info", "order_detail" /*缺啥补啥*/)
//    //维度表清单
//    val dimTables: Array[String] = Array[String]("user_info", "base_province" /*缺啥补啥*/)
    jsonStream.foreachRDD(rdd => {
      //每一批数据进行redis的操作
      val redisFactKey: String = "FACT:TABLES"
      val redisDimKey: String = "DIM:TABLES"

      val jedis: Jedis = MyRedisUtils.getRedisFromPool()
      //从redis读取事实表和纬度表
      val factTables: util.Set[String] = jedis.smembers(redisFactKey)
      println(s"factTables:$factTables")
      //把事实表做成广播变量
      val factTableBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(factTables)

      val dimTables: util.Set[String] = jedis.smembers(redisDimKey)
      println(s"dimTables:$dimTables")
      val dimTableBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dimTables)

      jedis.close()
      rdd.foreachPartition(records => {
        val jedis1: Jedis = MyRedisUtils.getRedisFromPool()
        records.foreach(jsonObj => {
          //获取数据类型
          val opType: String = jsonObj.getString("type")
          val op: String = opType match {
            case "bootstrap-insert" => "I"
            case "insert" => "I"
            case "update" => "U"
            case "delete" => "D"
            case _ => null
          }
          if (op != null) {
            val tablesName: String = jsonObj.getString("table")
            //如果包含事实表
            if (factTableBC.value.contains(tablesName)) {
              val data: String = jsonObj.getString("data")
              //封装topic
              val factTopicName: String = s"DWD_${tablesName.toUpperCase()}_${op}_20220629"
              MyKafkaUtil.send(factTopicName, data)
            }
            //纬度表封装到redis中
            if (dimTableBC.value.contains(tablesName)) {
              val dimObj: JSONObject = jsonObj.getJSONObject("data")
              val dimID: String = dimObj.getString("id")
              //封装redis key
              val dimKey: String = s"DIM:${tablesName.toUpperCase()}:$dimID"
              jedis1.set(dimKey, dimObj.toJSONString)
            }
          }
        })
        jedis1.close()
        MyKafkaUtil.flush()
      }
      )
      MyOffsetsUtils.saveOffset(groupId, topic, offsetRanges)
    })

    ssc.start()
    ssc.awaitTermination();
  }
}
