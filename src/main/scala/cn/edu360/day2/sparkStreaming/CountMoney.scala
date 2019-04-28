package cn.edu360.day2.sparkStreaming


import com.alibaba.fastjson.JSON
import com.google.gson.Gson
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.util.{LinkedHashMap => JLinkedHashMap}


object CountMoney {
    case class log(data:String, phoneNum:String, amount :Int,  lat:Double, log:Double,province:String,city:String,district:String)

    def main(args: Array[String]): Unit = {
        val group = "g0011"
        val topic = "recharge"
        //创建SparkConf，如果将任务提交到集群中，那么要去掉.setMaster("local[2]")
        val conf = new SparkConf().setAppName("DirectStream").setMaster("local[2]")
        //创建一个StreamingContext，其里面包含了一个SparkContext
        val streamingContext = new StreamingContext(conf, Seconds(5));

        //配置kafka的参数
        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> "hdp1:9092,hdp2:9092,hdp3:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> group,
            "auto.offset.reset" -> "earliest", // lastest
            "enable.auto.commit" -> (false: java.lang.Boolean)
        )

        val topics = Array(topic)
        //在Kafka中记录读取偏移量
        val stream = KafkaUtils.createDirectStream[String, String](
            streamingContext,
            //位置策略
            PreferConsistent,
            //订阅的策略
            Subscribe[String, String](topics, kafkaParams)
        )

        stream.map(record => record.value()).foreachRDD(rdd => {
            val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
            import spark.implicits._
            val df = spark.read.json(spark.createDataset(rdd))
          //  val df = ds.select(from_json('value, ArrayType(schema)) as "value").select(explode('value)).select($"col.*")

//            import org.apache.spark.sql.functions._
//            import spark.implicits._
//            df.select(date_format($"time".cast(DateType), "yyyyMMdd").as("day"), $"*").show()


            df.show()
//            df.createTempView("moneyByTerminal")
//            //执行SQL
//            val r1 = spark.sql("SELECT sum(amount) money TFROM moneyByTerminal where type='wx'")
//            r1.show()

        })

        streamingContext.start()
        streamingContext.awaitTermination()
    }
}
