package pers.prover07

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    parallelismRdd()
  }

  def demo1(): Unit = {
    // 构建SparkConf对象，并设置本地运行和程序的名称
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    // 通过SparkConf对象构建SparkContext对象
    val sc = new SparkContext(conf)
    // 读取文件，生成 RDD 对象
    val fileRdd: RDD[String] = sc.textFile("data/word.txt", 4)
    // 将单词进行切割，得到一个存储全部单词的集合对象
    val wordsRdd: RDD[String] = fileRdd.flatMap(_.split(" "))
    // 将单词转换成 （"hello"->("hello",1)） 元组，后面可以根据 key 聚合 value
    val wordsRddTuple: RDD[(String, Int)] = wordsRdd.map((_, 1))
    // 将元祖的value按照key进行分组，并对该组所有的value进行聚合操作
    val resultRdd: RDD[(String, Int)] = wordsRddTuple.reduceByKey(_ + _)
    // 收集数据
    val wordCount: Array[(String, Int)] = resultRdd.collect()
    // 输出
    wordCount.foreach(println)
  }

  /**
   * 创建 rdd 方式一：基于内存创建
   */
  def createRddInMemory(): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sparkContext = new SparkContext(conf)

    // 方式一：调用 parallelize() 函数调用, 将本地集合转换为分布式RDD
    val parallelizeRdd : RDD[Int] = sparkContext.parallelize(List(1, 2, 3, 4))

    // 方式二，调用makeRdd，底层也是调用 parallelize 实现
    val makeRDD: RDD[Int] = sparkContext.makeRDD(List(1, 2, 3, 4))

    // 方式三：调用 wholeTextFiles，读取一个文件夹下的多个小文件
    val tinyFiles: RDD[(String, String)] = sparkContext.wholeTextFiles("data/files")
  }

  /**
   * 设置 rdd 运算的并行任务数
   */
  def parallelismRdd(): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sparkContext = new SparkContext(conf)
    // 创建 RDD 时在后面执行并行任务的数量
    val list: RDD[Int] = sparkContext.makeRDD(List(1, 2, 3, 4), 4)

    list.collect().foreach(println)
  }
}
