package com.serendipity.gmall.realtme.app

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.serendipity.gmall.realtme.bean.{PageActionLog, PageDisplayLog, PageLog, StartLog}
import com.serendipity.gmall.realtme.util.{MyKafkaUtil, MyOffsetsUtils, MyRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 数据分流：
 * 1.创建StreamingContext上下文环境
 *
 * 2.消费kafka数据
 *
 * 3.装换数据
 * 通用的数据结构：Map或者JSONObject
 * 专用的数据结构：bean对象
 *
 * 4.写入到DWD层,分流：将数据拆分到不同的主题中
 * 启动主题： DWD_START_LOG
 * 页面访问主题：DWD_PAGE_LOG
 * 页面动作主题：DWD_PAGE_ACTION
 * 页面曝光主题：DWD_PAGE_DISPLAY
 * 错误主题：DWD_ERROR_INFO
 */

object OdsBaseLogApp {

  def main(args: Array[String]): Unit = {
//    System.setProperty("hadoop.home.dir", "D:\\soft\\hadoop")
    //local[4] 的原因是为了保持和kafka分区一致
    val conf: SparkConf = new SparkConf().setAppName("ods_base_log_app").setMaster("local[1]")

    val topic:String = "ODS_BASE_LOG"
    val groupId:String="g1"

    //微批次，每5秒钟执行一次
    val ssc = new StreamingContext(conf, Seconds(5))
    //1.先去redis取出offset
    val offsetMap: Map[TopicPartition, Long] = MyOffsetsUtils.readOffset(topic, groupId)
    //根据offset去kafka中获取kafkaDStream
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.nonEmpty) {
        //如果redis中有offset
      kafkaDStream = MyKafkaUtil.getKafkaDStream(topic, ssc, groupId, offsetMap)
    }else{
        //如果redis中没有offset,就默认消费
      kafkaDStream = MyKafkaUtil.getKafkaDStream(topic, ssc, groupId)
    }
    //从kafkaDStream中获取这批数据最新的offset保存到redis中，但不对流进行处理
    var offsetRanges: Array[OffsetRange] = null;
    val offsetDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })
    //kafkaDStream.print()
    //如果是这种情况进行打印，因为没有进行序列化，所以要转成jsonObject会报异常：java.io.NotSerializableException: org.apache.kafka.clients.consumer.ConsumerRecord
    val jsonDStream: DStream[JSONObject] = offsetDStream.map(record => {
      val str: String = record.value()
      val jSONObject: JSONObject = JSON.parseObject(str)
      jSONObject
    })
    //jsonDStream.print(1000)

    /*3.2 分流
        日志数据：
          页面访问数据：
              公共字段
              页面数据
              曝光数据
              事件数据
              错误数据
          启动数据
              公共字段
              启动数据
              错误数据
    */
    //页面访问
    val DWD_PAGE_LOG_TOPIC: String = "DWD_PAGE_LOG_TOPIC_20220629"
    //页面曝光
    val DWD_PAGE_DISPLAY_TOPIC: String = "DWD_PAGE_DISPLAY_TOPIC_20220629"
    //页面事件
    val DWD_PAGE_ACTION_TOPIC: String = "DWD_PAGE_ACTION_TOPIC_20220629"
    //启动数据
    val DWD_START_LOG_TOPIC: String = "DWD_START_LOG_TOPIC_20220629"
    //错误数据
    val DWD_ERROR_LOG_TOPIC = "DWD_ERROR_LOG_TOPIC_20220629"
    //分流规则：
    //错误数据：不做任何的拆分，只要包含错误字段，直接整条数据发送到对应的topic
    //页面数据：拆分页面访问，曝光，事件，分别发送到对应的topic
    //启动数据：发送到对应的topic
    //从jsonDStream遍历的所有的RDD,foreachRDD是一个action算子
    jsonDStream.foreachRDD(
      rdd => {
        //每一个rdd处理过程中包含多调数据
        println("----")
        rdd.foreach(
          jsonObj => {
            //分流过程
            //先去分流错误数据
            val errObj: JSONObject = jsonObj.getJSONObject("err")
            //如果不等于空
            if (errObj != null) {
              //将错误数据发送到DWD_ERROR_LOG_TOPIC_20220629
              MyKafkaUtil.send(DWD_ERROR_LOG_TOPIC, jsonObj.toJSONString)
            } else {
              //提取公共字段
              val commonObj: JSONObject = jsonObj.getJSONObject("common")
              /*
                "ar":"2",
                "ba":"Xiaomi",
                "ch":"xiaomi",
                "is_new":"0",
                "md":"Xiaomi Mix2 ",
                "mid":"mid_172",
                "os":"Android 11.0",
                "uid":"94",
                "vc":"v2.1.134"
               */
              val ar: String = commonObj.getString("ar")
              val ba: String = commonObj.getString("ba")
              val ch: String = commonObj.getString("ch")
              val isNew: String = commonObj.getString("is_new")
              val md: String = commonObj.getString("md")
              val mid: String = commonObj.getString("mid")
              val os: String = commonObj.getString("os")
              val vc: String = commonObj.getString("vc")
              val uid: String = commonObj.getString("uid")
              //提取时间戳
              val ts: Long = jsonObj.getLong("ts")
              //页面数据
              val pageObj: JSONObject = jsonObj.getJSONObject("page")
              if (pageObj != null) {
                /*
                  "during_time":10435,
                  "item":"22",
                  "item_type":"sku_id",
                  "last_page_id":"good_list",
                  "page_id":"good_detail",
                  "source_type":"promotion"
                 */
                //提取page字段
                val pageId: String = pageObj.getString("page_id")
                val pageItem: String = pageObj.getString("item")
                val pageItemType: String = pageObj.getString("item_type")
                val lastPageId: String = pageObj.getString("last_page_id")
                val sourceType: String = pageObj.getString("source_type")
                val duringTime: Long = pageObj.getLong("during_time")
                var pageLog = PageLog(mid, uid, ar, ch, isNew, md, os, vc, ba, pageId, lastPageId, pageItem, pageItemType, duringTime, sourceType, ts)
                MyKafkaUtil.send(DWD_PAGE_LOG_TOPIC, JSON.toJSONString(pageLog, new SerializeConfig(true)))
                //提取曝光数据
                val displaysJsonArr: JSONArray = jsonObj.getJSONArray("displays")
                if (displaysJsonArr != null && displaysJsonArr.size() > 0) {
                  displaysJsonArr.forEach(display => {
                    //类型强转
                    //val displayObj: JSONObject = display[JSONObject]
                    val displayObj: JSONObject = classOf[JSONObject].cast(display)
                    //提取曝光字段
                    val displayType: String = displayObj.getString("display_type")
                    val displayItem: String = displayObj.getString("item")
                    val displayItemType: String = displayObj.getString("item_type")
                    val posId: String = displayObj.getString("pos_id")
                    val order: String = displayObj.getString("order")
                    val pageDisplayLog =
                      PageDisplayLog(mid, uid, ar, ch, isNew, md, os, vc, ba, pageId, lastPageId, pageItem, pageItemType, duringTime, sourceType, displayType, displayItem, displayItemType, order, posId, ts)
                    // 写到 DWD_PAGE_DISPLAY_TOPIC
                    MyKafkaUtil.send(DWD_PAGE_DISPLAY_TOPIC, JSON.toJSONString(pageDisplayLog, new SerializeConfig(true)))
                  })
                }
                //提取事件数据
                val actionArray: JSONArray = jsonObj.getJSONArray("actions")
                if (actionArray != null && actionArray.size() > 0) {
                  actionArray.forEach(action => {
                    val actionObj: JSONObject = classOf[JSONObject].cast(action)
                    val actionId: String = actionObj.getString("action_id")
                    val actionItem: String = actionObj.getString("item")
                    val actionItemType: String = actionObj.getString("item_type")
                    val actionTs: Long = actionObj.getLong("ts")
                    //封装PageActionLog
                    var pageActionLog =
                      PageActionLog(mid, uid, ar, ch, isNew, md, os, vc, ba, pageId, lastPageId, pageItem, pageItemType, duringTime, sourceType, actionId, actionItem, actionItemType, actionTs, ts)
                    //写出到DWD_PAGE_ACTION_TOPIC
                    MyKafkaUtil.send(DWD_PAGE_ACTION_TOPIC, JSON.toJSONString(pageActionLog, new SerializeConfig(true)))
                  })
                }
              }
              //启动数据
              val startJsonObj: JSONObject = jsonObj.getJSONObject("start")
              if (startJsonObj != null) {
                //提取字段
                val entry: String = startJsonObj.getString("entry")
                val loadingTime: Long = startJsonObj.getLong("loading_time")
                val openAdId: String = startJsonObj.getString("open_ad_id")
                val openAdMs: Long = startJsonObj.getLong("open_ad_ms")
                val openAdSkipMs: Long = startJsonObj.getLong("open_ad_skip_ms")
                //封装StartLog
                var startLog =
                  StartLog(mid, uid, ar, ch, isNew, md, os, vc, ba, entry, openAdId, loadingTime, openAdMs, openAdSkipMs, ts)
                //写出DWD_START_LOG_TOPIC
                MyKafkaUtil.send(DWD_START_LOG_TOPIC, JSON.toJSONString(startLog, new SerializeConfig(true)))
              }
            }
          }
        )
        MyOffsetsUtils.saveOffset(groupId, topic, offsetRanges)
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }
}
