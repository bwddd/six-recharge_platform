package com.zyh.recharge_p.test

import com.zyh.recharge_p.utils.DBUtil

/**
  * @description $description
  * @Author: 张英豪
  * @Create: 2019-04-02 17:30:39
  * @pp: 一直在颓废
  **/
object TestUpdate {
  def main(args: Array[String]): Unit = {
    println(DBUtil.updateEveryHour("1",2 ,2))
  }
}
