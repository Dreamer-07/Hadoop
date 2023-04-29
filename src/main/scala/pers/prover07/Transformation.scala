package pers.prover07

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 转换算子
 */
object Transformation {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("map")
    val sc = new SparkContext(conf)

    testGroupByKey(sc)

    sc.stop()
  }

  /**
   * map
   *
   * @param sc
   */
  def testMap(sc: SparkContext): Unit = {
    val rdd: RDD[Int] = sc.parallelize(List(1, 2, 3, 4))

    // 定义函数
    def toMap(x: Int): Int = {
      if (x % 2 != 0) {
        return x
      } else {
        return x * 2
      }
    }

    // 调用函数
    val mapRdd: RDD[Int] = rdd.map(toMap)

    println(mapRdd.collect().mkString(","))
  }

  /**
   * flatMap：将集合中的每一个元素按照一定的规则进行拆解，拆解后的内容作为原集合的一个元素
   *
   * @param sc
   */
  def testFlatMap(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.parallelize(List("a b c", "a a d", "b c d"))

    val flatMapRdd: RDD[String] = rdd.flatMap(_.split(" "))
    println(flatMapRdd.collect().mkString(","))
  }

  /**
   * 适用于 Key-Value 类型的RDD，自动按照 key 进行分组，然后根据提供的函数 func 的逻辑进行聚合操作，完成组内所有value的聚合操作。
   *
   * @param sc
   */
  def testReduceByKey(sc: SparkContext): Unit = {
    val rdd: RDD[(String, Int)] = sc.parallelize(List(("a", 1), ("a", 3), ("c", 2), ("b", 1), ("c", 1)))

    val rdd1: RDD[(String, Int)] = rdd.reduceByKey(_ + _)

    println(rdd1.collect().mkString(","))
  }

  /**
   * 过滤数据(true保留， false过滤)
   *
   * @param sc
   */
  def testFilter(sc: SparkContext): Unit = {
    val rdd: RDD[Int] = sc.parallelize(List(1, 2, 4, 5, 7, 10))

    val result: Array[Int] = rdd.filter(_ % 2 == 0).collect()

    println(result.mkString("Array(", ", ", ")"))
  }

  /**
   * 去重(针对key-value型的rdd，只有key和value的值都相同时认定为重复元素)
   *
   * @param sc
   */
  def testDistinct(sc: SparkContext): Unit = {
    val rdd: RDD[Int] = sc.parallelize(List(1, 2, 3, 2, 5, 2))

    val result: Array[Int] = rdd.distinct().collect()

    println(result.mkString("Array(", ", ", ")"))
  }

  /**
   * 将同一个分区的数据直接转换为相同类型的内存数组进行处理，得到的结果分区不变
   *
   * @param sc
   */
  def testGlom(sc: SparkContext): Unit = {
    val rdd: RDD[Int] = sc.parallelize(List(1, 3, 4, 2, 6, 5), 3)

    val result: Array[Array[Int]] = rdd.glom().collect()

    result.foreach(res => println(res.mkString(",")))
  }

  /**
   * 根据rdd中元素（通常也是元祖）的指定内容进行分组，分组后的元素都是二元组。
   *
   * @param sc
   */
  def testGroupBy(sc: SparkContext): Unit = {
    val rdd: RDD[(String, Int)] = sc.parallelize(Array(("a", 1), ("a", 4), ("b", 2), ("a", 3), ("b", 5), ("b", 6)))

    // 根据 tuple 的第一个元素分组
    val result: Array[(String, Iterable[(String, Int)])] = rdd.groupBy(tup => tup._1).collect()

    result.foreach(res => println(res._1 + ":" + res._2.mkString(",")))
  }

  /**
   * 根据 key 将 value 进行分组
   *
   * @param sc
   */
  def testGroupByKey(sc: SparkContext): Unit = {
    val rdd: RDD[(String, Int)] = sc.parallelize(Array(("a", 1), ("a", 4), ("b", 2), ("a", 3), ("b", 5), ("b", 6)))

    val result: Array[(String, List[Int])] = rdd.groupByKey().map(tup => (tup._1, tup._2.toList)).collect()

    result.foreach(res => println(res._1 + ":" + res._2.mkString(",")))
  }

  /**
   * 分区排序
   *
   * @param sc
   */
  def testSortBy(sc: SparkContext): Unit = {
    val rdd: RDD[(Char, Int)] = sc.parallelize((List(('w', 2), ('h', 5), ('k', 9), ('m', 3), ('a', 7),
      ('p', 4), ('q', 1), ('n', 8), ('y', 6))))
    // 根据哪个值排序，是否升序，分区数量
    val res1: Array[Array[(Char, Int)]] = rdd.sortBy(tup => tup._2, ascending = true, numPartitions = 3).glom().collect()
    val res2: Array[(Char, Int)] = rdd.sortBy(tup => tup._2, ascending = true, numPartitions = 3).collect()
  }

}
