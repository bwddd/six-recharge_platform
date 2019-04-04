package com.zyh.recharge_p.utils

import redis.clients.jedis.{Jedis, JedisPool}

/**
  * @description $description
  * @Author: 张英豪
  * @Create: 2019-04-02 12:16:30
  * @pp: 一直在颓废
  **/
object JedisPool {
  val pool = new JedisPool("hadoop01", 6379)

  /**
    * 返回一个jedis连接
    * @return
    */
  def getJedis: Jedis = {
    pool.getResource
  }

}
