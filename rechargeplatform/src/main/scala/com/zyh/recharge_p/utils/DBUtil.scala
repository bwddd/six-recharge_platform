package com.zyh.recharge_p.utils

import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs

/**
  * @description $description
  * @Author: 张英豪
  * @Create: 2019-04-01 19:01:58
  * @pp: 一直在颓废
  **/
object DBUtil {
  // 加载mysql配置
  DBs.setup()

  /**
    * 查询以前消费的偏移量信息
    * @param topic 消费主题
    * @param consumerGroupName 消费者组名称
    * @return 偏移量集合
    */
  def offsetQuery(topic: String, consumerGroupName: String): List[Int] = {
    DB.readOnly { implicit session =>
      SQL("select * from direct_to_mysql where topic = ? and consumerGroup = ? order by partitionIndex")
        .bind(topic, consumerGroupName)
        .map(_.int("offset")).list().apply()
    }
  }

  /**
    * 查询数据库中消费记录
    * @param topic 消费主题
    * @param consumerGroupName 消费者组名称
    * @param partitionIndex 分区号
    * @return 记录数
    */
  def recordQuery(topic: String, consumerGroupName: String, partitionIndex: Int): Option[Int] = {
    DB.readOnly { implicit session =>
      SQL("select count(*) cnt from direct_to_mysql where topic = ? and consumerGroup = ? and partitionIndex = ?")
        .bind(topic, consumerGroupName, partitionIndex).map(_.int("cnt")).single().apply()
    }
  }

  /**
    * 更新offset值
    * @param topic 消费主题
    * @param consumerGroupName 消费者组名称
    * @param partitionIndex 分区号
    * @param offset 偏移量
    * @return 返回
    */
  def updateOffset(topic: String, consumerGroupName: String, partitionIndex: Int, offset: Int, isUp: Boolean = true): Int = {
    if (isUp){
      DB.autoCommit { implicit session =>
        SQL("update direct_to_mysql set offset = ? where topic = ? and consumerGroup = ? and partitionIndex = ?")
          .bind(offset, topic, consumerGroupName, partitionIndex).update().apply()
      }
    }else{
      DB.autoCommit { implicit session =>
        SQL("insert into direct_to_mysql values(null, ?, ?, ?, ?)")
          .bind(topic, consumerGroupName, partitionIndex, offset).update().apply()
      }
    }
  }


  /**
    * 插入省份失败表
    * @param province 省份
    * @param failureNum 失败数量
    * @return
    */
  def updateProvinceFailure(day: Int, hour:Int, province:String, failureNum: Int, isUp: Boolean = true) = {
    if (isUp){
      DB.autoCommit { implicit session =>
        SQL("update province_failure set failurenum = ? where province = ? and hour = ? and day = ?")
          .bind(failureNum, province, hour, day).update().apply()
      }
    }else{
      DB.autoCommit { implicit session =>
        SQL("insert into province_failure values(null, ?, ?, ?, ?)")
          .bind(province, failureNum, hour, day).update().apply()
      }
    }

  }

  /**
    * 查询是否有返回 省份失败表
    * @param provience 省份
    * @return 对应数量
    */
  def provinceQuery(day: Int,hour:Int, provience: String): Option[Int] = {
    DB.readOnly { implicit session =>
      SQL("select count(*) cnt from province_failure where province = ? and hour = ? and day = ?")
        .bind(provience, hour, day).map(_.int("cnt")).single.apply()
    }
  }


  /**
    * 查看top表中数据
    * @param province 省份
    * @return
    */
  def topQuery(province: String): Option[Int] = {
    DB.readOnly { implicit session =>
      SQL("select count(*) cnt from top10_province where province = ?")
        .bind(province).map(_.int("cnt")).single.apply()
    }
  }

  /**
    * 更新top值
    * @param province 省份
    * @param totalAmount 总量
    * @param successRate 成功率
    */
  def updateTop(province: String, totalAmount: Int, successRate: String, isUp: Boolean = true) = {
    if(isUp){
      DB.autoCommit {implicit session =>
        SQL("update top10_province set totalAmount = ? , successRate = ? where province = ?")
          .bind(totalAmount, successRate, province).update().apply()
      }
    }else{
      DB.autoCommit {implicit session =>
        SQL("insert into top10_province values(null, ?, ?, ?)")
          .bind(province, totalAmount, successRate).update().apply()
      }
    }
  }

  /**
    * 查看 实时充值情况(每小时)
    * @param dayHour 每小时
    * @return 符合条件数量
    */
  def everyHourQuery(dayHour: String): Option[Int] = {
    DB.readOnly { implicit session =>
      SQL("select count(*) cnt from every_hour_recharge where dayhour = ?")
        .bind(dayHour).map(_.int("cnt")).single.apply()
    }
  }

  /**
    * 更新实时充值情况(每小时)
    * @param dayHour 每小时
    * @param totalAmount 总订单
    * @param sum 总金额
    * @param isUp 是否更新
    * @return 是否成功
    */
  def updateEveryHour(dayHour: String, totalAmount: Int, sum: Long, isUp: Boolean = true): Int = {
    if (isUp){
      DB.autoCommit {implicit session =>
        SQL("update every_hour_recharge set totalAmount = ? , sum = ? where dayhour = ?")
          .bind(totalAmount, sum, dayHour).update().apply()
      }
    }else{
      DB.autoCommit {implicit session =>
        SQL("insert into every_hour_recharge values(null, ?, ?, ?)")
          .bind(dayHour, totalAmount, sum).update().apply()
      }
    }
  }





  /*----------------------------------------- 离线处理 ----------------------------------------------------------------*/
  /**
    * 离线插入数据每天省份的充值失败量
    * @param day 每天
    * @param province 省份
    * @param failureAmount 失败量
    * @param failRate 占比
    * @return 插入情况
    */
  def offlineFailureInsert(day: String, province: String, failureAmount: Int, failRate: String): Int = {
    DB.autoCommit( implicit session => {
      SQL("insert into offline_failure values(null, ?, ?, ?, ?)")
        .bind(day, province, failureAmount, failRate).update().apply()
    })
  }

}
