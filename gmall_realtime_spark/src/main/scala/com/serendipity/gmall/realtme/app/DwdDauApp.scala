package com.serendipity.gmall.realtme.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.serendipity.gmall.realtme.bean.{DauInfo, PageLog}
import com.serendipity.gmall.realtme.util.{MyEsUtils, MyKafkaUtil, MyOffsetsUtils, MyRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.jackson.{Json, renderJValue}
import redis.clients.jedis.{Jedis, Pipeline}

import java.lang
import java.text.SimpleDateFormat
import java.time.{LocalDate, Period}
import java.util.Date
import scala.collection.mutable.ListBuffer

object DwdDauApp {

  def main(args: Array[String]): Unit = {

    //还原状态
    revertState()

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
      println("检查后：" + pageLogs.size)
      pageLogs.iterator
    })
    //纬度关联
    val dauInfoStream: DStream[DauInfo] = redisFilterDStream.mapPartitions(pageIter => {
      //结果集
      val listBuffer: ListBuffer[DauInfo] = ListBuffer[DauInfo]()

      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val jedis: Jedis = MyRedisUtils.getRedisFromPool()

      for (page <- pageIter) {

        val dauInfo = new DauInfo()


        //拼接redis中key
        val user_id: String = page.user_id
        val redisUidKey: String = s"DIM:USER_INFO:$user_id"
        val str: String = jedis.get(redisUidKey)
        //拿到json对象
        val jsonObj: JSONObject = JSON.parseObject(str)
        val gender: String = jsonObj.getString("gender")
        val birthday: String = jsonObj.getString("birthday")

        //换算年龄
        val birthdayLd: LocalDate = LocalDate.parse(birthday)
        val now: LocalDate = LocalDate.now()

        val years: Int = Period.between(birthdayLd, now).getYears
        dauInfo.user_age = years.toString
        dauInfo.user_gender = gender

        //关联地区信息
        val provinceID: String = dauInfo.province_id
        val redisProvinceKey: String = s"DIM:BASE_PROVINCE:$provinceID"
        val provinceJson: String = jedis.get(redisProvinceKey)
        val provinceJsonObj: JSONObject = JSON.parseObject(provinceJson)
        val provinceName: String = provinceJsonObj.getString("name")
        val provinceIsoCode: String = provinceJsonObj.getString("iso_code")
        val province3166: String = provinceJsonObj.getString("iso_3166_2")
        val provinceAreaCode: String = provinceJsonObj.getString("area_code")

        //补充到对象中
        dauInfo.province_name = provinceName
        dauInfo.province_iso_code = provinceIsoCode
        dauInfo.province_3166_2 = province3166
        dauInfo.province_area_code = provinceAreaCode

        //2.3  日期字段处理
        val date: Date = new Date(page.ts)
        val dtHr: String = dateFormat.format(date)
        val dtHrArr: Array[String] = dtHr.split(" ")
        val dt: String = dtHrArr(0)
        val hr: String = dtHrArr(1).split(":")(0)
        //补充到对象中
        dauInfo.dt = dt
        dauInfo.hr = hr

        listBuffer.append(dauInfo)
      }

      jedis.close()
      //返回结果集
      listBuffer.iterator
    }
    )

    //写入到ES中
    dauInfoStream.foreachRDD(rdd => {

      //这个区间是每一批数据driver端的执行逻辑
      rdd.foreachPartition(
        iter => {

          val dauList: List[(String, DauInfo)] = iter.map(dauInfo => (dauInfo.mid, dauInfo))
            .toList
          if (dauList.size > 0) {
            val head: (String, DauInfo) = dauList.head
            val ts: Long = head._2.ts
            val dateStr: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date(ts))
            val indexName = s"gmall_dau_info_$dateStr"
            MyEsUtils.bulkSave(indexName, dauList)
          }
        }
      )

      MyOffsetsUtils.saveOffset(groupId, topic, offsetRanges)

    })


    ssc.start()
    ssc.awaitTermination()

  }


  /**
   * 状态还原
   * 在每次启动实时任务时，进行一次状态还原,以ES为例，将所有的mid其他区出来，覆盖到redis中
   */
  def revertState(): Unit = {

    val date: LocalDate = LocalDate.now()
    val indexName = s"gmall_dau_info_$date"

    val fieldName: String = "mid"
    val mids: List[String] = MyEsUtils.searchField(indexName, fieldName)

    val jedis: Jedis = MyRedisUtils.getRedisFromPool()
    val redisKey: String = s"DAU:$date"
    jedis.del(redisKey)

    if (mids != null && mids.size > 0) {
      val pipeline: Pipeline = jedis.pipelined()
      for (mid <- mids) {
        pipeline.sadd(redisKey, mid)
      }
      pipeline.sync()
    }
    jedis.close();
  }
}
