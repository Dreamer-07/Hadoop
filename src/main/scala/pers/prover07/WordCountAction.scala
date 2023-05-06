package pers.prover07

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCountAction {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("map")
    val sc = new SparkContext(conf)
//    val rdd: RDD[String] = sc.parallelize(List("Stephen Curry", "Kevin Durant", "Kobe Bryant", "Curry", "Curry", "Curry", "Curry", "Curry", "Curry", "Curry"))

//    wordCount5(rdd)

    val rdd: RDD[Int] = sc.parallelize(List[Int]())
    val user = new User()

    rdd.foreach(num => {
      println(s"age = ${user.age} + $num")
    })

    sc.stop()

  }

  case class User(){
    var age: Int = 30
  }

  def wordCount1(rdd: RDD[String]): Unit = {
    val result: RDD[(String, Int)] = rdd.flatMap(str => str.split(" "))
      .groupBy(word => word)
      .mapValues(items => items.size)

    println(result.collect().mkString(","))
  }

  def wordCount2(rdd: RDD[String]): Unit = {
    val result: RDD[(String, Int)] = rdd.flatMap(_.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    println(result.collect().mkString(","))
  }

  def wordCount3(rdd: RDD[String]): Unit = {
    /*
    * aggregateByKey 是 flod 的升级版本，它适用于分区内与分区间计算过程不同的场景
    * */
    val result: RDD[(String, Int)] = rdd.flatMap(_.split(" "))
      .map(word => (word, 1))
      .aggregateByKey(0)((r1, r2) => {
        println(s"分区内数据: $r1, $r2")
        r1 + r2
      }, (r1, r2) => {
        println(s"分区间数据: $r1, $r2")
        r1 + r2
      })

    println(result.collect().mkString(","))
  }

  def wordCount4(rdd: RDD[String]): Unit = {

    val result: RDD[(String, Int)] = rdd.flatMap(_.split(" "))
      .map(word => (word, 1))
      .combineByKey(
        // 其实比起 aggregateByKey， 主要是多一个函数可以在进行分区计算前将数据格式进行转换，其他的没啥区别
        v => v,
        (r1: Int, r2: Int) => {
          println(s"分区内数据: $r1, $r2")
          r1 + r2
        }, (r1: Int, r2: Int) => {
          println(s"分区间数据: $r1, $r2")
          r1 + r2
        }
      )

    println(result.collect().mkString(","))
  }

  def wordCount5(rdd: RDD[String]): Unit = {
    val result: collection.Map[String, Long] = rdd.flatMap(_.split(" "))
      .map(word => (word, 1)).countByKey()

    println(result)
  }



}
