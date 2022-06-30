package com.serendipity.gmall.realtme.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis
import java.util
import scala.collection.mutable

/**
 *
 * offset管理工具类：用于往redis中存储和读取offset
 *
 *
 * offset管理方案：
 *      1.后置提交offset：先处理数据后提交 -> 手动控制偏移量的提交
 *      2.手动控制偏移量提交，SparkStreaming 提供了手动提交方案，但是我们不能用，因为我们会对DStream的结构进行转换，所以不能用
 *      3.手动提取偏移量到redis
 *          -> 从kafka中消费到数据，先提取偏移量offset
 *          -> 等到数据处理成功之后，将末尾offset存储到redis
 *          -> 从kafka中消费数据之前，先到redis中获取下一波数据的起始offset,使用读取到的offset去kafka中消费数据
 *          -> 当消费完之后，再获取数据末尾的offset存到offset中
 *
 *
 *
 *              往Redis中存储offset
 * 问题： 存的offset从哪来？
 *            从消费到的数据中提取出来的，传入到该方法中。
 *            offsetRanges: Array[OffsetRange]
 *        offset的结构是什么？
 *            Kafka中offset维护的结构
 *               groupId + topic + partition => offset
 *            从传入进来的offset中提取关键信息
 *        在redis中怎么存?
 *          类型: hash
 *          key : groupId + topic
 *          value: partition - offset  ， partition - offset 。。。。
 *          写入API: hset / hmset
 *          读取API: hgetall
 *          是否过期: 不过期
 */
object MyOffsetsUtils {


  /**
   * 将offset保存到redis中
   *
   * @param groupId
   * @param topic
   * @param offsetRanges :为什么是一个数组？？因为是按照一个topic来的
   *
   *    val topic: String,
   *    val partition: Int,
   *    val fromOffset: Long,
   *    val untilOffset: Long) extends Serializable
   *
   *
   */
  def saveOffset(groupId: String, topic: String, offsetRanges: Array[OffsetRange]): Unit = {

    if (offsetRanges != null && offsetRanges.size > 0) {
      val offsetMap: util.HashMap[String, String] = new util.HashMap[String, String]()
      offsetRanges.foreach(
        offsetRange => {
          val partition: Int = offsetRange.partition
          //需要的保存的offset
          val endOffset: Long = offsetRange.untilOffset
          offsetMap.put(partition.toString, endOffset.toString)
        }
      )
      //打印需要提交的offset
      println("提交的offset:" + offsetMap)
      val jedis: Jedis = MyRedisUtils.getRedisFromPool()
      //封装一下redis中的key
      val key: String = s"offset:$topic:$groupId"
      //保存到redis中
      jedis.hset(key, offsetMap)
      jedis.close()
    }
  }


  /**
   * 从redis中读取offset
   *
   * @param groupId
   * @param topic
   * @return
   */
  def readOffset(groupId: String, topic: String): Map[TopicPartition, Long] = {
    val key: String = s"offset:$topic:$groupId"
    val jedis: Jedis = MyRedisUtils.getRedisFromPool()
    //获取的这个Map是java中hashMap,所以需要将java中hashMap转成scala中的Map
    val offsetMap: util.Map[String, String] = jedis.hgetAll(key)
    println("读取到的offset:"+offsetMap)
    val results: mutable.Map[TopicPartition, Long] = mutable.Map[TopicPartition, Long]()

    offsetMap.forEach((partition,offset)=>{
        results.put(new TopicPartition(topic,partition.toInt),offset.toLong)
    })
    jedis.close()
    results.toMap
  }
}
