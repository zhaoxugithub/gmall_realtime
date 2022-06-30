package com.serendipity.gmall.realtme.util

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.util.Properties
import scala.collection.mutable

/**
 * kafka工具类，用于生产和消费
 */
object MyKafkaUtil {

  //kafka消费者参数
  private val kafkaConsumerParams: mutable.Map[String, String] = mutable.Map(
    //kafka连接工具
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> PropertiesUtils(MyConfig.KAFKA_BOOTSTRAP_SERVERS),
    //kafka中key的序列化方式
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    //kafka中value的序列化方式
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    //指定消费者group
    ConsumerConfig.GROUP_ID_CONFIG -> "gmall",
    //earliest：从最早的offset开始消费，就是partition的起始位置开始消费
    //latest：从最近的offset开始消费，就是新加入partition的消息才会被消费
    //none：报错
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
    //是否开启自动提交offset功能,kafka自身去维护offset
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true"
  )

  //kafka生产者参数
  private val kafkaProducerParams: mutable.Map[String, String] = mutable.Map(
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> PropertiesUtils(MyConfig.KAFKA_BOOTSTRAP_SERVERS),
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer",
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer",
    //开启幂等性：所谓的幂等性就是指Producer不论向Server发送多少次重复数据，Server端都只会持久化一条
    ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG -> "true"
  )
  /**
   * 默认offset消费
   *
   * @param topic
   * @param ssc
   * @param groupId
   * @return
   */
  def getKafkaDStream(topic: String, ssc: StreamingContext, groupId: String): InputDStream[ConsumerRecord[String, String]] = {
    /*
      ssc: StreamingContext :SparkStreamContext上下文
      locationStrategy: LocationStrategy，位置策略，PreferBrokers，PreferConsistent
                  PreferBrokers：Use this only if your executors are on the same nodes as your Kafka brokers.
                  PreferConsistent：Use this in most cases, it will consistently distribute partitions across all executors.
      consumerStrategy: ConsumerStrategy[K, V]：消费者策略
     */
    //自定义一个消费者
    kafkaConsumerParams(ConsumerConfig.GROUP_ID_CONFIG) = groupId
    val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
      //  topics: Iterable[jl.String],
      //  kafkaParams: collection.Map[String, Object])
      //  offsets: collection.Map[TopicPartition, Long])
      ConsumerStrategies.Subscribe(Array(topic), kafkaConsumerParams))
    dStream
  }

  /**
   * 指定位置消费
   *
   * @param topic
   * @param ssc
   * @param groupId
   * @param offset
   */
  def getKafkaDStream(topic: String, ssc: StreamingContext, groupId: String, offset: collection.Map[TopicPartition, Long]): InputDStream[ConsumerRecord[String, String]] = {
    kafkaConsumerParams(ConsumerConfig.GROUP_ID_CONFIG) = groupId
    val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe(Array(topic), kafkaConsumerParams, offset))
    dStream
  }

  private val producer: KafkaProducer[String, String] = createKafkaProducer()

  /**
   * 创建kafka生产者
   *
   * @return
   */
  def createKafkaProducer(): KafkaProducer[String, String] = {
    val kafkaProducerProperties = new Properties()
    kafkaProducerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, PropertiesUtils(MyConfig.KAFKA_BOOTSTRAP_SERVERS))
    kafkaProducerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProducerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    //开启幂等性：所谓的幂等性就是指Producer不论向Server发送多少次重复数据，Server端都只会持久化一条
    kafkaProducerProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    kafkaProducerProperties.put(ProducerConfig.ACKS_CONFIG, "all")
    /*
    ack级别:acks  默认1
    缓冲区大小:batch.size 默认16k
    触发时间：linger.ms  默认5
    重试次数：retries  默认0
     */
    val producer = new KafkaProducer[String, String](kafkaProducerProperties)
    producer
  }

  /**
   * 生产数据
   *
   * @param topic
   * @param msg
   */
  def send(topic: String, msg: String): Unit = {
    producer.send(new ProducerRecord[String, String](topic, msg))
  }
  /**
   * 发送数据指定key
   *
   * @param topic
   * @param msg
   * @param key
   */
  def send(topic: String, msg: String, key: String): Unit = {
    producer.send(new ProducerRecord[String, String](topic, key, msg))
  }

  /**
   * 刷写缓冲区
   *
   * @param args
   */
  def flush(): Unit = {
    if (producer != null) {
      producer.flush()
    }
  }

  /**
   * 关闭生产者
   */
  def close(): Unit = {
    if (producer != null) {
      producer.close()
    }
  }
}
