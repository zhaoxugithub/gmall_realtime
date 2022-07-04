package com.serendipity.gmall.realtme.app

import com.alibaba.fastjson.JSON
import com.serendipity.gmall.realtme.bean.PageLog
import com.serendipity.gmall.realtme.util.{MyKafkaUtil, MyOffsetsUtils, MyRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.jackson.Json
import redis.clients.jedis.Jedis

import java.lang
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer

object DwdDauApp {

  def main(args: Array[String]): Unit = {
    //准备环境
    val conf: SparkConf = new SparkConf().setAppName("dwd_dau_app").setMaster("local[1]")
    val ssc = new StreamingContext(conf, Seconds(5))

    //定义topic
    val topic: String = "DWD_PAGE_LOG_TOPIC_20220629"
    val groupId: String = "DWD_PAGE_LOG_TOPIC_20220629 "

    //从redis中获取offset
    val offsetMap: Map[TopicPartition, Long] = MyOffsetsUtils.readOffset(groupId, topic)
    var kafkaStream: InputDStream[ConsumerRecord[String, String]] = null
    //如果offset不为空
    if (offsetMap != null || offsetMap.nonEmpty) {
      kafkaStream = MyKafkaUtil.getKafkaDStream(topic, ssc, groupId, offsetMap)
    } else {
      kafkaStream = MyKafkaUtil.getKafkaDStream(topic, ssc, groupId)
    }

    //获取完kafkaStream,保存kafka流的offset的结束点
    var offsetRanges: Array[OffsetRange] = null
    val offsetStream: DStream[ConsumerRecord[String, String]] = kafkaStream.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })

    //转换算子，封装计算逻辑
    val pageLogStream: DStream[PageLog] = offsetStream.map(consumerRecord => {
      val str: String = consumerRecord.value()
      val pageLog: PageLog = JSON.parseObject(str, classOf[PageLog])
      pageLog
    })

    //触发行动算子,打印
    //    pageLogStream.print(1000)
    //rdd count 统计的是条目数
    pageLogStream.cache()
    pageLogStream.foreachRDD(rdd => println("自我审查前：" + rdd.count()))
    //去重
    //自我审查：将页面访问数据中的last_page_id不为空的数据过滤掉, 保留为空的数据,意思是第一次页面访问的数据
    val filterDStream: DStream[PageLog] = pageLogStream.filter(pageLog => pageLog.last_page_id == null)

    filterDStream.cache()
    filterDStream.foreachRDD(rdd => {
      println("自我审查后：" + rdd.count())
      println("--------------------")
    })

    //第三方审查：通过自身去重，然后再和redis比较，减少redis并发负载
    val redisFilterDStream: DStream[PageLog] = filterDStream.mapPartitions(iterator => {

      val pageLogIterator: List[PageLog] = iterator.toList
      println("检查前数量:" + pageLogIterator.size)

      val pageLogs: ListBuffer[PageLog] = ListBuffer[PageLog]()
      val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

      val jedis: Jedis = MyRedisUtils.getRedisFromPool()

      for (pageLog <- pageLogIterator) {

        val mid: String = pageLog.mid
        val date: Date = new Date(pageLog.ts)
        val dateStr: String = format.format(date)
        val redisDauKey = s"DAU:$dateStr"

        //判断包含和写入实现了原子操作
        val isNew: lang.Long = jedis.sadd(redisDauKey, mid)
        //如果是新的数据，就加入
        if (isNew == 1L) {
          pageLogs.append(pageLog);
        }
      }
      jedis.close()
      println("检查后："+pageLogs.size)
      pageLogs.iterator
    })

    //纬度关联




    ssc.start()
    ssc.awaitTermination()

  }
}
