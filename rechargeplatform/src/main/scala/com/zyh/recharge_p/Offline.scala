package com.zyh.recharge_p

import com.alibaba.fastjson.JSON
import com.zyh.recharge_p.utils.DBUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @description $description
  * @Author: 张英豪
  * @Create: 2019-04-03 10:48:16
  * @pp: 一直在颓废
  **/
object Offline {
  def main(args: Array[String]): Unit = {
    /*
      1.业务失败省份 TOP3（离线处理[每天]）（存入MySQL）
      以省份为维度统计每个省份的充值失败数,及失败率存入MySQL中。
    */
    val conf = new SparkConf().setMaster("local[*]").setAppName("offLine statistics")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val jsonFile: RDD[String] = sc.textFile("data/cmcc.json")

    val city = sc.textFile("data/city.txt").map(line => {
      val sp = line.split(" ")
      (sp(0), sp(1))
    }).collect.toMap
    // 广播省份代码
    val cityCode = sc.broadcast(city).value

    jsonFile.map(line => {
      val jsob = JSON.parseObject(line)
      val day = jsob.get("requestId").toString.substring(0,8)
      val province = cityCode.get(jsob.get("provinceCode").toString)
      val bussinessRst = jsob.get("bussinessRst").toString

      ((day, province), bussinessRst)
    }).groupByKey.map(info => {
      var totalAmount = 0
      var failureAmount = 0
      info._2.foreach(ln => {
        if (!ln.equals("0000")){
          failureAmount += 1
        }
        totalAmount += 1
      })
      val failRate = (failureAmount.toDouble / totalAmount.toDouble).formatted("%.2f")
      (info._1._1, info._1._2.get, failureAmount, failRate, totalAmount)
    }).sortBy(_._3, ascending = false).take(3).foreach(ln => {
      DBUtil.offlineFailureInsert(ln._1, ln._2, ln._3, ln._4)
      println(s"day: ${ln._1} || province: ${ln._2} || failCount: ${ln._3} || count: ${ln._5} || failRate: ${ln._4}")
    })


  }
}
