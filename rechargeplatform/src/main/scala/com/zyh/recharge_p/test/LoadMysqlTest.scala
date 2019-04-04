package com.zyh.recharge_p.test

import scalikejdbc.config.DBs
import scalikejdbc.{DB, SQL}

/**
  * @description $description
  * @Author: 张英豪
  * @Create: 2019-04-01 18:13:56
  * @pp: 一直在颓废
  **/
object LoadMysqlTest {

  def main(args: Array[String]): Unit = {
    // 默认加载 db.default.* 的配置
    DBs.setup()

    /*----------------------- 查询数据 ----------------------*/
    val list: List[String] = DB.readOnly { implicit session =>
      SQL("select * from direct_to_mysql").map(_.string("id")).list().apply()
    }

    list.foreach(println)



    /*----------------------- 插入数据 ----------------------*/
    val insertResult = DB.autoCommit {implicit session =>
      SQL("insert into direct_to_mysql values(null, ?, ?, ?, ?)")
        .bind("recharge", "zyh", 1, 10).update().apply()
    }

    println("插入结果: " + insertResult)

    // 插入并返回主键标识,  updateAndReturnGeneratedKey(xx) default主键
    val insertRs = DB.autoCommit {implicit session=>
      SQL("insert into direct_to_mysql values(null, ?, ?, ?, ?)")
        .bind("recharge", "wu", 2, 22)
        .updateAndReturnGeneratedKey().apply()
    }
    println("插入成功后返回数据(主键id): " + insertRs)
  }

}
