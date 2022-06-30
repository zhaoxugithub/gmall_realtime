package com.serendipity.gmall.realtme.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * redis工具类，用于连接redis,操作
 */
object MyRedisUtils {

  //创建redis连接池
  var jedisPool: JedisPool = null;

  def getRedisFromPool(): Jedis = {

    //如果连接池是空，需要创建连接池
    if (jedisPool == null) {
      val config = new JedisPoolConfig()
      //线程最大空闲
      config.setMaxIdle(20)
      //线程最小空闲
      config.setMinIdle(20)
      //最大连接数
      config.setMaxTotal(100)
      //忙碌时线程是否进行等待
      config.setBlockWhenExhausted(true)
      //忙碌时线程进行等待的事件
      config.setMaxWaitMillis(5000)
      //连接是否进行测试
      config.setTestOnBorrow(true)
      val host: String = PropertiesUtils(MyConfig.REDIS_HOST)
      val port: String = PropertiesUtils(MyConfig.REDIS_PORT)
      jedisPool = new JedisPool(config, host, port.toInt)
    }
    jedisPool.getResource
  }
}
