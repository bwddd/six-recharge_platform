package com.zyh.recharge_p

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.zyh.recharge_p.utils.{DBUtil, JedisPool}
import com.zyh.recharge_p.utils.DBUtil._
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.{StringDecoder, StringEncoder}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

import scala.tools.cmd.Spec


/**
  * @description $description
  * @Author: 张英豪
  * @Create: 2019-04-01 18:53:57
  * @pp: 一直在颓废
  **/
object KafkaDirectStreaming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("kafka direct streaming")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    sc.setCheckpointDir("data/checkpoint")
    val cityDict = sc.textFile("data/city.txt").map(line => {
      val sp = line.split(" ")
      (sp(0), sp(1))
    }).collect().toMap
    // 广播地区参照表
    val cityBroadcast = sc.broadcast(cityDict)
    val ss = new StreamingContext(sc, Seconds(5))

    /*------------------------- 配置kafka连接信息 ----------------------------*/
    val groupName = "zxc1"
    val topic = "recharge"
    val brokerList = "hadoop01:9092, hadoop02:9092, hadoop03:9092"

    val topics: Set[String] = Set(topic)

    /*------------------------- 查询上一次消费位置 ----------------------------*/
    val offsetInfos: List[Int] = offsetQuery(topic, groupName)


    /*------------------------- 准备kafka连接参数 ----------------------------*/
    val kafkaParam = Map(
      "metadata.broker.list" -> brokerList,
      "group.id" -> groupName,
      "auto.offset.reset" -> kafka.api.OffsetRequest.SmallestTimeString
    )


    /*------------------------- 创建容器存放offset ----------------------------*/
    // 创建一个容器存放offset
    var fromOffset: Map[TopicAndPartition, Long] = Map()

    // 创建 kafka连接对象
    var kafkaStream: InputDStream[(String, String)] = null

    if (offsetInfos.nonEmpty) {
      // 遍历每个结果
      for (i <- offsetInfos.indices) {
        val tp = TopicAndPartition(topic, i)
        fromOffset += tp -> offsetInfos(i)
      }

      // 将消息进行transform
      val messageHandler = (msg: MessageAndMetadata[String, String]) => {
        (msg.key(), msg.message())
      }

      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ss, kafkaParam, fromOffset, messageHandler)
    } else {
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ss, kafkaParam, topics)
    }


    // 偏移量范围
    var offsetRange = Array[OffsetRange]()

    // 从rdd中获取偏移量
    val transform = kafkaStream.transform(rdd => {
      offsetRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })

    /*----------------------------------------- 1. 业务概况 (结果入redis) --------------------------------------------------------*/
    // 1) 统计全网的充值订单量, 充值金额, 充值成功数
    // 2) 实时充值业务办理趋势, 主要统计全网的订单量数据
    val jsonResult = transform.transform(rdd => {
      rdd.map(line => {
        val json = JSON.parseObject(line._2)
        val sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS")
        // 充值结果
        val bussinessRst = json.get("bussinessRst").toString
        // 充值金额
        val chargefee = json.get("chargefee").toString.toInt
        // 开始时间
        val startTimeStr = json.get("requestId").toString
        val startTime = sdf.parse(startTimeStr.substring(0, 17))
        // 结束时间
        val endTimeStr = json.get("receiveNotifyTime")
        var endTime = startTime
        if (endTimeStr != null) {
          endTime = sdf.parse(endTimeStr.toString)
        }
        // 日期
        val simpleDateFormat = new  SimpleDateFormat("yyyy-MM-dd")
        val day = simpleDateFormat.format(startTime)
        // 地区
        val provinceCode = cityBroadcast.value.get(json.get("provinceCode").toString)
        // 服务名称
        val serviceName = json.get("serviceName").toString
        (day, bussinessRst, chargefee, startTime, endTime, provinceCode, serviceName)
      })
    }).filter(_._7.equals("reChargeNotifyReq")).cache()

    /*----------------------------------------- 1. 统计全网的充值订单量, 充值金额, 充值成功数 --------------------------------------------------------*/
    recharge_one(jsonResult)

    /*----------------------------------------- 2. 业务质量（l） --------------------------------------------------------*/
    recharge_two(jsonResult)

    /*----------------------------------------- 3. 充值订单省份 TOP10 (存入mysql) --------------------------------------------------------*/
    recharge_three(jsonResult)

    /*----------------------------------------- 4. 实时充值情况分布（存入MySQL） --------------------------------------------------------*/
    recharge_four(jsonResult)


    /*----------------------------------------- 6. 充值机构分布 --------------------------------------------------------*/
    recharge_six(jsonResult)

    // 释放缓存
    sc.getPersistentRDDs.foreach(_._2.unpersist())

    /*--------------------------------------  维护偏移量  ------------------------------------------------*/
    transform.foreachRDD(rdd => {
      // 维护偏移量
      for (o <- offsetRange) {
        val lists = recordQuery(topic, groupName, o.partition)
        if (lists.head > 0) {
          updateOffset(topic, groupName, o.partition, o.untilOffset.toInt)
        } else {
          updateOffset(topic, groupName, o.partition, o.untilOffset.toInt, isUp = false)
        }
      }
    })

    ss.start()
    ss.awaitTermination()
  }


  /**
    *   1. 统计全网的充值订单量, 充值金额, 充值成功数 结果存入 redis
    * @param jsonResult json解析后的数据DStream
    */
  def recharge_one(jsonResult: DStream[(String, String, Int, Date, Date, Option[String], String)]): Unit ={
    // 1) 统计全网的充值订单量, 充值金额, 充值成功数
    jsonResult.foreachRDD(rdd => {
      rdd.groupBy(_._1).map(info => {
        var sum = 0
        var totalAmount = 0
        var successAmount = 0
        var totalTime = 0L
        info._2.foreach(in => {
          if (in._2.equals("0000")) {
            successAmount += 1
            sum += in._3
          }
          totalTime += in._5.getTime - in._4.getTime
          totalAmount += 1
        })
        (totalAmount, sum, successAmount, totalTime, info._1)
      }).foreachPartition(info => {
        // 将计算结果, 总订单量, 总充值金额, 成功数量
        val jedis = JedisPool.getJedis
        info.foreach(info => {
          var totalAmount = info._1
          var sum = info._2.toLong
          var successAmount = info._3
          var totalTime = info._4
          val day = info._5

          // 对以前数据进行累加
          totalAmount = jedis.hincrBy(day,"totalAmount", totalAmount).toString.toInt
          sum = jedis.hincrBy(day,"sum", sum).toString.toLong
          successAmount = jedis.hincrBy(day,"successAmount", successAmount).toString.toInt
          totalTime = jedis.hincrBy(day,"totalTime", totalTime).toString.toLong

          val successRate = (successAmount.toDouble / totalAmount.toDouble).formatted("%.5f")
          val avgRechargeTime = (totalTime.toDouble / totalAmount.toDouble) / 1000

          // 成功率
          jedis.hset(day, "successRate", successRate)
          // 平均时间
          jedis.hset(day, "avgRechargeTime", avgRechargeTime.toString)
          println(s"插入成功: totalAmount = $totalAmount || sum = $sum || successAmount = $successAmount" +
            s" || successRate = $successRate% || avgRechargeTime = $avgRechargeTime 秒/单")
        })
        jedis.close()
      })
    })
  }

  /**
    *  2. 全国各省充值业务失败量分布
    *     统计每小时各个省份的充值失败数据量
    * @param jsonResult json解析后的数据DStream
    */
  def recharge_two(jsonResult: DStream[(String, String, Int, Date, Date, Option[String], String)]) = {
    // 全国各省充值业务失败量分布
    // 统计每小时各个省份的充值失败数据量
    val spec = StateSpec.function((key: (Int, Int, String), countOfThisBatch: Option[Int], state: State[Int]) => {
      // 计算word当前值状态
      var newState = countOfThisBatch.getOrElse(0) + state.getOption().getOrElse(0)
      // 更新word的状态值
      state.update(newState)
      // 返回
      (key, newState)
    })
    jsonResult.map(in => {
      var count = 0
      if (!in._2.equals("0000")) count += 1
      val hour = in._4.getHours
      val day = in._4.getDay
      ((day, hour, in._6 get), count)
    }).reduceByKey(_ + _)
      .mapWithState(spec)
      .foreachRDD(rdd => {
        rdd.foreach(item => {
          // 判断是否存在
          val ints: Option[Int] = provinceQuery(item._1._1, item._1._2, item._1._3)
          if (ints.getOrElse(0) > 0) {
            updateProvinceFailure(item._1._1, item._1._2, item._1._3, item._2)
          } else {
            updateProvinceFailure(item._1._1, item._1._2, item._1._3, item._2, isUp = false)
          }
          println(s"城市: ${item._1}, 失败数: ${item._2}")
        })
      })
  }


  /**
    *  3. 以省份为维度统计订单量排名前 10 的省份数据,并且统计每个省份的订单成功率，只保留一位小数，存入MySQL中
    * @param jsonResult json解析后的数据DStream
    */
  def recharge_three(jsonResult: DStream[(String, String, Int, Date, Date, Option[String], String)]) = {
    // 以省份为维度统计订单量排名前 10 的省份数据,并且统计每个省份的订单成功率，只保留一位小数，存入MySQL中
    jsonResult.foreachRDD(rdd => {
      rdd.map(info => {
        (info._6.get, info._2)
      }).groupByKey.map(info => {
        var sum = 0
        var totalAmount = 0
        var successAmount = 0
        var totalTime = 0L
        info._2.foreach(in => {
          if (in.equals("0000")) {
            successAmount += 1
          }
          totalAmount += 1
        })
        val rate = (successAmount.toDouble / totalAmount.toDouble).toString
        // 计算成功率
        (info._1, totalAmount, rate)
      }).sortBy(_._2, ascending = false).take(10).foreach(item => {
        // 对数据进行查询, 查看数据库中是否有数据
        if (topQuery(item._1).getOrElse(0)> 0) {
          updateTop(item._1, item._2, item._3)
        } else {
          updateTop(item._1, item._2, item._3, isUp = false)
        }
        println(s"province: ${item._1} || totalAmout: ${item._2} || successRate: ${item._3}")
      })
    })
  }


  /**
    *  4. 实时充值情况分布（存入MySQL）
    * @param jsonResult json解析后的数据DStream
    */
  def recharge_four(jsonResult: DStream[(String, String, Int, Date, Date, Option[String], String)]) = {
    // 进行历史计算
    val specFour = StateSpec.function((key: String, countOfThisBatch: Option[(Int, Int)], state: State[(Int, Int)]) => {
      // 计算word当前值状态
      var newStateTotalAmount = countOfThisBatch.getOrElse((0,0))._1 + state.getOption().getOrElse((0,0))._1
      var newStateSum = countOfThisBatch.getOrElse((0,0))._2 + state.getOption().getOrElse((0,0))._2
      // 更新word的状态值
      state.update((newStateTotalAmount,newStateSum))
      // 返回
      (key, (newStateTotalAmount,newStateSum))
    })
    jsonResult.transform(rdd => {
      rdd.map(info => {
        (info._4.getDay + "-" + info._4.getHours, info._3)
      }).groupByKey.map(info => {
        var sum = 0
        var totalAmount = 0
        info._2.foreach(in => {
          sum += in
          totalAmount += 1
        })
        (info._1, (totalAmount, sum))
      })
    }).mapWithState(specFour).foreachRDD(rdd => {
      rdd.foreach(ln => {
        val ints = everyHourQuery(ln._1)
        if (ints.head > 0){
          updateEveryHour(ln._1, ln._2._1, ln._2._2)
        }else{
          updateEveryHour(ln._1, ln._2._1, ln._2._2, isUp = false)
        }
        println(s"day_hour: ${ln._1} || totalAmouont: ${ln._2._1} || sum: ${ln._2._2}")
      })

    })
  }


  /**
    *   6. 充值机构分布   总量 / 分钟   总量 / 小时
    *   以省份为维度,统计每分钟各省的充值笔数和充值金额
    *   以省份为维度,统计每小时各省的充值笔数和充值金额
    * @param jsonResult json解析后的数据DStream
    */
  def recharge_six(jsonResult: DStream[(String, String, Int, Date, Date, Option[String], String)]) = {
    // mapWithState 实现
    val sp_six = StateSpec.function((key: (String, String), currThisBatch: Option[(Int, Int, Long, Long)], state:  State[(Int, Int, Long, Long)]) => {
      val newState_ever_M_Amount = currThisBatch.getOrElse((0,0,0L,0L))._1 + state.getOption().getOrElse((0,0,0L,0L))._1
      val newState_ever_H_Amount = currThisBatch.getOrElse((0,0,0L,0L))._2 + state.getOption().getOrElse((0,0,0L,0L))._2
      val newState_ever_M_Sum = currThisBatch.getOrElse((0,0,0L,0L))._3 + state.getOption().getOrElse((0,0,0L,0L))._3
      val newState_ever_H_Sum = currThisBatch.getOrElse((0,0,0L,0L))._4 + state.getOption().getOrElse((0,0,0L,0L))._4

      state.update((newState_ever_M_Amount, newState_ever_H_Amount, newState_ever_M_Sum, newState_ever_H_Sum))

      (key, (newState_ever_M_Amount, newState_ever_H_Amount, newState_ever_M_Sum, newState_ever_H_Sum))
    })

    // updateStateByKey 实现
    val up_byKey = (curr:Seq[(Int, Int, Long, Long)], pre: Option[(Int, Int, Long, Long)]) => {
      var newState_ever_M_Amount  =  pre.getOrElse((0,0,0L,0L))._1
      var newState_ever_H_Amount =  pre.getOrElse((0,0,0L,0L))._2
      var newState_ever_M_Sum  =  pre.getOrElse((0,0,0L,0L))._3
      var newState_ever_H_Sum =  pre.getOrElse((0,0,0L,0L))._4
      curr.foreach(curr => {
        newState_ever_M_Amount += curr._1
        newState_ever_H_Amount += curr._2
        newState_ever_M_Sum += curr._3
        newState_ever_H_Sum += curr._4
      })
      Option(newState_ever_M_Amount, newState_ever_H_Amount, newState_ever_M_Sum, newState_ever_H_Sum)
    }

    jsonResult.map(info => {
      val simpleDateFormat = new  SimpleDateFormat("yyyy-MM-dd")
      val day = simpleDateFormat.format(info._4)
      ((day,info._6), info._3)
    }).groupByKey.map(info => {
      val minute = 60 * 60
      val hour = minute * 60
      var totalAmount = 0
      var sum = 0L
      info._2.foreach(ln => {
        totalAmount += 1
        sum += ln
      })
      // 每分钟充值笔
      val ever_M_Amount = totalAmount / minute
      // 每小时充值笔
      val ever_H_Amount = totalAmount / hour
      // 每分钟充值金额
      val ever_M_Sum = sum / minute
      // 每小时充值金额
      val ever_H_Sum = sum / hour
      ((info._1._1, info._1._2.get), (ever_M_Amount, ever_H_Amount, ever_M_Sum, ever_H_Sum))
    })/*.mapWithState(sp_six).print()*/
      .updateStateByKey(up_byKey).print
  }
}
