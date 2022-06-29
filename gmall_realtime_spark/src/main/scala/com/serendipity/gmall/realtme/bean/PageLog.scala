package com.serendipity.gmall.realtme.bean

/*
  scala 中case class 和class 的区别：
    1.初始化的时候可以不用new，当然你也可以加上，普通类一定需要加new
    2、toString的实现更漂亮；
    3、默认实现了equals 和hashCode；
    4、默认是可以序列化的，也就是实现了Serializable ；
    5、自动从scala.Product中继承一些函数;
　　 6、case class构造函数的参数是public级别的，我们可以直接访问；
    7.支持模式匹配；
　　   其实感觉case class最重要的特性应该就是支持模式匹配。这也是我们定义case class的唯一理由
 */
//页面日志
case class PageLog(
                    mid :String,
                    user_id:String,
                    province_id:String,
                    channel:String,
                    is_new:String,
                    model:String,
                    operate_system:String,
                    version_code:String,
                    brand : String ,
                    page_id:String ,
                    last_page_id:String,
                    page_item:String,
                    page_item_type:String,
                    during_time:Long,
                    sourceType : String ,
                    ts:Long

                  ) {

}
