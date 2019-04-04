package org.qianfeng.kafka_direct_streaming


import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs


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
      SQL("select * from direct_to_mysql where topic = ? and consumerGroupName = ? order by partitionIndex")
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
  def recordQuery(topic: String, consumerGroupName: String, partitionIndex: Int): List[Int] = {
    DB.readOnly { implicit session =>
      SQL("select count(*) cnt from direct_to_mysql where topic = ? and consumerGroupName = ? and partitionIndex = ?")
        .bind(topic, consumerGroupName, partitionIndex).map(_.int("cnt")).list().apply()
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
        SQL("update direct_to_mysql set offset = ? where topic = ? and consumerGroupName = ? and partitionIndex = ?")
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
    * 插入
    * @param province
    * @param failureNum
    * @return
    */
  def updateProvinceFailure(hour:Int, province:String, failureNum: Int, isUp: Boolean = true) = {
    if (isUp){
      DB.autoCommit { implicit session =>
        SQL("update province_failure set failurenum = ? where province = ? and hour = ?")
          .bind(failureNum, province, hour).update().apply()
      }
    }else{
      DB.autoCommit { implicit session =>
        SQL("insert into province_failure values(null, ?, ?, ?)")
          .bind(province, failureNum, hour).update().apply()
      }
    }

  }

  /**
    * 查询是否有返回
    * @param provience
    * @return
    */
  def provinceQuery(hour:Int, provience: String): List[Int] = {
    DB.readOnly { implicit session =>
      SQL("select count(*) cnt from province_failure where province = ? and hour = ?")
        .bind(provience, hour).map(_.int("cnt")).list().apply()
    }
  }
}
