package cn.edu360.day2.sparkSQL

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.{DataFrame, SparkSession}

object TerminalMoney {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().appName("JdbcDataSource")
                .master("local[*]")
                .getOrCreate()

        import spark.implicits._

        //指定以后读取json类型的数据(有表头)
        val jsons: DataFrame = spark.read.json("hdfs://hdp1:9000/bike/recharge/20190404")

        jsons.createTempView("moneyByTerminal")
        //执行SQL
        //
        val r1 = spark.sql("SELECT sum(amount) money TFROM moneyByTerminal where type='wx'")
        r1.show()

        val r2 = spark.sql("SELECT sum(amount) moneyD FROM moneyByTerminal group by district")
        r2.show()


        spark.stop()

    }

}
