package com.serendipity.gmall.realtme.util

import java.util.ResourceBundle

//配置解析类
object PropertiesUtils {


  //利用ResourceBundle对象读取properties
  private val resourceBundle: ResourceBundle = ResourceBundle.getBundle("config")

  def apply(key: String): String = {
    resourceBundle.getString(key)
  }

  def main(args: Array[String]): Unit = {
    println(apply("kafka.broker.list"))
  }
}
