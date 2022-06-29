package com.serendipity.gmall.realtme.bean

/*
{
  "actions": [
    {
      "action_id": "cart_remove",
      "item": "34",
      "item_type": "sku_id",
      "ts": 1656378264895
    }
  ],
  "common": {
    "ar": "6",
    "ba": "iPhone",
    "ch": "Appstore",
    "is_new": "0",
    "md": "iPhone Xs",
    "mid": "mid_59",
    "os": "iOS 13.2.3",
    "uid": "97",
    "vc": "v2.1.134"
  },
  "page": {
    "during_time": 3790,
    "last_page_id": "good_detail",
    "page_id": "cart"
  },
  "ts": 1656378263000
}
 */
//页面动作日志
case class PageActionLog(
                          mid :String,
                          user_id:String,
                          province_id:String,
                          channel:String,
                          is_new:String,
                          model:String,
                          operate_system:String,
                          version_code:String,
                          brand:String ,
                          page_id:String ,
                          last_page_id:String,
                          page_item:String,
                          page_item_type:String,
                          during_time:Long,
                          sourceType:String ,
                          action_id:String,
                          action_item:String,
                          action_item_type:String,
                          action_ts :Long ,
                          ts:Long
                        ) {

}
