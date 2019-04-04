package org.qianfeng.kafka_direct_streaming

import java.text.SimpleDateFormat
import java.util.Date

import org.qianfeng.kafka_direct_streaming.DBUtil._
import com.alibaba.fastjson.{JSON, JSONObject}
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.{StringDecoder, StringEncoder}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import redis.clients.jedis.{Jedis, JedisPool}
import scalikejdbc.{DB, SQL}

/**
  * @description $description
  * @author: 屈建华 - 翻版必究
  * @create: 2019-04-01 20:36:38
  **/
object Direct_Spark_Streaming {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("yarn").setAppName("kafka direct streaming")
    val sc = new SparkContext(conf)
    val city = sc.textFile("file:///city.txt").map(x => {
      val arr = x.split(" ")
      val cityId = arr(0)
      val city = arr(1)
      (cityId,city)
    }).collect().toMap
    // 广播城市表
    val bc = sc.broadcast(city)
    val ssc = new StreamingContext(sc, Seconds(5))

    /**------------------------- 配置kafka连接信息 ----------------------------**/
    val topic = "qq"
    val groupName = "2"
    val brokerList = "hadoop-01:9092,hadoop-02:9092,hadoop-03:9092"

    val topics: Set[String] = Set(topic)

    /**------------------------- 查询上一次消费位置 ----------------------------**/
    val offsetInfos: List[Int] = offsetQuery(topic, groupName)


    /**------------------------- 准备kafka连接参数 ----------------------------**/
    val kafkaParam = Map(
      "metadata.broker.list" -> brokerList,
      "group.id" -> groupName,
      "auto.offset.reset" -> kafka.api.OffsetRequest.SmallestTimeString
    )


    /**------------------------- 创建容器存放offset ----------------------------**/
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

      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParam, fromOffset, messageHandler)
    } else {
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, topics)
    }


    // 偏移量范围
    var offsetRange = Array[OffsetRange]()

    // 从rdd中获取偏移量
    val transform = kafkaStream.transform(rdd => {
      offsetRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })

    ssc.checkpoint("file:///checkpoint")

    val res = transform.transform{rdd =>
      rdd.map(line => {
        val json = JSON.parseObject(line._2)
        val sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS")
        val bussinessRst = json.get("bussinessRst").toString.trim
        val chargefee = json.get("chargefee").toString.toInt
        val provinceID =json.get("provinceCode").toString
        val startTimeStr = json.get("requestId").toString.trim.substring(0, 17)
        val startTime = sdf.parse(startTimeStr)
        val endTimeStr = json.get("receiveNotifyTime")
        var endTime:Date = startTime
        if (endTimeStr != null){
            endTime = sdf.parse(endTimeStr.toString.trim)
        }
        //通过ID从被广播的city表中获取省份
        val province = bc.value.get(provinceID).get
        //金额 成功 开始时间 结束时间 省份
        (chargefee, bussinessRst, startTime, endTime,province)
      })
  }.cache()

    /**----- 统计全网的充值订单量, 充值金额, 充值成功数, 充值平均时长, 充值成功率 -----**/
    res.foreachRDD(x => {
      x.map{x =>
      (1,(x._1,x._2,x._3,x._4,x._5))
    }.groupByKey().map({x =>
      //全部订单量
      var countAll = 0
      //成功数
      var succNum = 0
      //总金额
      var money = 0L
      //总时长
      var time = 0L
      //成功率
      var succPer = 0.0
      for (i <- x._2.toList){
        if (i._2.equals("0000")){
          //成功数+1
          succNum+=1
        }
        //金额
        money+=i._1
        //总时长
        time += i._4.getTime-i._3.getTime
      }
      //总订单量
      countAll = x._2.toList.size
      //成功率
      succPer = succNum / countAll
      //全部订单量 成功数 成功率 平均时长 总金额
      (countAll,succNum,succPer,time/countAll,money)
    }).foreach(x => {
        val redis = new JedisPool("hadoop-01",6379)
        val pool = redis.getResource
        //全部订单量
        pool.incrBy("countAll",x._1)
        //成功数
        pool.incrBy("succNum",x._2)
        //成功率
        pool.incrBy("succPer",x._3.toInt)
        //平均时长
        pool.incrBy("timeAVG",x._4)
        //总金额
        pool.incrBy("time",x._5)
        pool.close()
      })
    })

    /**----- 全国各省充值业务失败量分布,统计每小时各个省份的充值失败数据量  -----**/
    val spec = StateSpec.function(mappingFunction = (k:String,v:Option[Int],h:State[Int]) =>{
      val sum = h.getOption().getOrElse(0) + v.getOrElse(0)
      h.update(sum)
      (k,sum)
    })
    res.transform(x => {
      x.filter(!_._2.equals("0000")).map(x => {
        (x._5+"-"+x._3.getHours,1)
      }).reduceByKey(_+_)
    }).mapWithState(spec).foreachRDD(x => x.foreach{ x =>
      //城市_每小时
      val pro_hourse = x.productIterator
      //写入MySQL
      val insertResult = DB.localTx{
        implicit session =>
          SQL("insert into province_hourse_dieNum(province,die_Num) values (?,?)").bind(x._1,x._2).update().apply()
      }
    })
    /**----- 充值订单量省份TOP10,每个省份的订单成功率 -----**/
    //每个省份订单量
    val cityOrder = res.transform(x => {
      x.map(x => {
        (x._5, 1)
      }).reduceByKey(_+_)
    }).mapWithState(spec)
    //每个城市的成功订单量
    val succCity = res.filter(_._2.equals("0000")).map(x => (x._5,1)).reduceByKey(_+_).mapWithState(spec)

    //每个城市的订单充值成功率
    succCity.join(cityOrder).map(x => {
      //成功率
      val succ = (x._2._1.toDouble / x._2._2.toDouble).formatted("%.3f")
      //省份 成功订单量 总订单量 成功率
      (x._1,x._2._1,x._2._2,succ)
    }).transform(_.sortBy(_._3)).foreachRDD(x => {
      x.take(10).foreach(x => {
        //写入MySQL
        val insertResult = DB.localTx{
          implicit session =>
            SQL("insert into province_OrderTop10(province,succNum,AnyNum,succPer) values (?,?,?,?)").bind(x._1,x._2,x._3,x._4).update().apply()
        }
      })
    })

    /**----- 实时统计每小时的充值笔数和充值金额 -----**/
    //每个小时的成功充值量
    val r1 = res.filter(_._2.equals("0000")).map(x => {
      //获取每个小时的充值量
      (x._3.getHours,1)
    }).reduceByKey(_+_).updateStateByKey(updateFunc,new HashPartitioner(ssc.sparkContext.defaultParallelism),true)
    //每个小时的充值金额
    val r2 = res.filter(_._2.equals("0000")).map(x => {
      (x._3.getHours,x._1)
    }).reduceByKey(_+_).updateStateByKey(updateFunc,new HashPartitioner(ssc.sparkContext.defaultParallelism),true)

    r1.join(r2).foreachRDD(x => {
      x.foreach(x => {
        val insertResult = DB.localTx{
          implicit session =>
            SQL("insert into hourse_succNum_money(timeHourse,succNum,money) values (?,?,?)").bind(x._1,x._2._1,x._2._2).update().apply()
        }
      })
    })

    /**----- 业务失败省份 TOP3（离线处理[每天]） -----**/
    lazy val ssb = SparkSession.builder().getOrCreate()
     val json = sc.textFile("file:///opt/test/cmcc.json")
    json.map(x => {
      val parse = JSON.parseObject(x)
      //省份
      val provinceCode = parse.get("provinceCode").toString
      val bussinessRst = parse.get("bussinessRst").toString
      //获取文件中城市ID对应的省份
      val province = bc.value.get(provinceCode).get
      (province,bussinessRst)
    }).groupByKey().map(x => {val sum = x._2.toList;(x._1,sum)}).map(x => {
      //定义变量统计失败量
      var dieCount = 0
      //定义变量统计全部订单量
      val sumCount = x._2.size
      for (i <- x._2){
        if (!i.equals("0000")){
          dieCount+=1
        }
      }
      //省份 失败数 失败率
      (x._1,(sumCount,(dieCount/sumCount).toDouble.formatted("%.5f")))
    }).foreach(x => {
      val insertResult = DB.localTx{
        implicit session =>
          SQL("insert into province_dieNum_diePer(province,dieNum,diePer) values (?,?,?)").bind(x._1,x._2._1,x._2._2).update().apply()
      }
    })

    /**----- 统计每分钟各省的充值笔数和充值金额 -----**/
    val rdd1 = res.map(x => {
      (x._5+""+x._3.getHours+"-"+x._3.getMinutes,x._1,x._2)
    }).filter(_._3.equals("0000"))
    //每分钟各省的充值笔数
      rdd1.map(x => {(x._1,1)}).reduceByKey(_+_).mapWithState(spec)
    //每分钟各省的充值金额
    val minMoney = rdd1.map(x => {(x._1,x._2)}).reduceByKey(_+_).mapWithState(spec).foreachRDD(x => {
      x.foreach(x => {
        //城市
        val pro = x._1
        //金额
        val money = x._2
        val insertResult = DB.localTx{
          implicit session =>
            SQL("insert into province_chargefee(province,chargefee) values (?,?)").bind(pro,money).update().apply()
        }
      })
    })
      // 维护偏移量
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
    ssc.start()
    ssc.awaitTermination()
  }


  val updateFunc = (it:Iterator[(Int,Seq[Int],Option[Int])]) => {
    it.map{x =>
      (x._1,x._2.sum + x._3.getOrElse(0))
    }
  }
}