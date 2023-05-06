package pers.prover07

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * 转换算子
 */
object Transformation {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("map")
    val sc = new SparkContext(conf)

    testSample(sc)

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

    res1.foreach(res => {
      res.foreach(r => print(r._1 + ":" + r._2 + ","))
      println()
    })
    println()
    res2.foreach(res => {
      println(res._1 + ":" + res._2)
    })
  }

  /**
   * 根据第一个键进行分区排序
   *
   * @param sc
   */
  def testSortByKey(sc: SparkContext): Unit = {
    val rdd: RDD[(Char, Int)] = sc.parallelize((List(('w', 2), ('h', 5), ('k', 9), ('m', 3), ('a', 7),
      ('p', 4), ('q', 1), ('n', 8), ('y', 6))))

    // 根据 key 进行排序
    val res1: Array[Array[(Char, Int)]] = rdd.sortByKey(ascending = false, numPartitions = 4).glom().collect()

    res1.foreach(res => {
      res.foreach(r => print(r._1 + ":" + r._2 + ","))
      println()
    })
  }

  /**
   * 并集
   *
   * @param sc
   */
  def testUnion(sc: SparkContext): Unit = {
    // 定义多个 rdd
    val rdd1: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4))
    val rdd2: RDD[Int] = sc.parallelize(Array(3, 4, 7, 8))

    // 注意：union 的计算必须是同类型的才可以
    val result: Array[Int] = rdd1.union(rdd2).collect()

    println(result.mkString(","))
  }

  /**
   * 计算交集和差集
   *
   * @param sc
   */
  def testIntersectionSubtract(sc: SparkContext): Unit = {
    // 定义多个 rdd
    val rdd1: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4))
    val rdd2: RDD[Int] = sc.parallelize(Array(3, 4, 7, 8))

    // 计算交集
    val res1: String = rdd1.intersection(rdd2).collect().mkString(",")
    println(res1)

    // 计算差集
    val res2: String = rdd1.subtract(rdd2).collect().mkString(",")
    println(res2)
  }

  def testJoin(sc: SparkContext): Unit = {
    val info: RDD[(Int, String)] = sc.parallelize(List((101, "saber"), (102, "arh"), (103, "lan")))
    val servlet: RDD[(Int, String)] = sc.parallelize(List((101, "daimao"), (102, "byq"), (104, "gongling")))
    // join - 通过 key 进行关联
    val res1: Array[(Int, (String, String))] = servlet.join(info).collect()
    println(res1.mkString(","))

    // leftJoin - 左外连接(显示左边)
    val res2: Array[(Int, (String, Option[String]))] = info.leftOuterJoin(servlet).collect()
    println(res2.mkString(","))

    // rightJoin - 右外连接(显示右边)
    val res3: Array[(Int, (Option[String], String))] = info.rightOuterJoin(servlet).collect()
    println(res3.mkString(","))
  }

  /**
   * 自定义分区规则
   */
  def testPartitionBy(sc: SparkContext): Unit = {
    val rdd: RDD[(String, Int)] = sc.parallelize(List(("andy", 1), ("jack", 1),
      ("hello", 1), ("lucy", 1), ("tom", 1), ("su", 1)))

    println("自定义分区之前:" + rdd.getNumPartitions)

    // 调用 partitioner 对 rdd 进行重新分区
    val rdd1: RDD[(String, Int)] = rdd.partitionBy(new Partitioner {
      // 分区数量
      override def numPartitions: Int = 3

      // 分区规则
      override def getPartition(key: Any): Int = {
        val firstChar: Char = key.toString.charAt(0)
        println("key:" + key)
        if (firstChar >= 'a' && firstChar <= 'i') {
          return 0
        } else if (firstChar >= 'j' && firstChar <= 'q') {
          return 1
        } else {
          return 2
        }
      }
    })

    // 分区
    val result: Array[Array[(String, Int)]] = rdd1.glom().collect()

    result.foreach(res => {
      println(res.mkString(","))
    })
  }

  /**
   * 与map类似，遍历的单位是每个partition上的数据。
   */
  def testMapPartitions(sc: SparkContext): Unit = {
    val rdd: RDD[(String, Int)] = sc.parallelize(List(("any", 1), ("jack", 1),
      ("hello", 1), ("lucy", 1), ("tom", 1), ("su", 1)), 3)

    // 定义一个处理函数
    def process(datas: Iterator[(String, Int)]): Iterator[(String, Int)] = {
      // 传递过来的分区之后的列表数据
      val result = ListBuffer[(String, Int)]()
      //遍历 datas
      for (ele <- datas) {
        val count: Int = ele._2 + 1
        result.append((ele._1, count))
      }
      // 返回处理好的分区数据
      result.iterator
    }

    val result: Array[Array[(String, Int)]] = rdd.mapPartitions(process).glom().collect()

    result.foreach(res => println(res.mkString(",")))
  }

  def testSample(sc: SparkContext): Unit = {
    val rdd: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7))

    /**
     * withReplacement：是否有放回取样，即样本是否可以重复抽取，默认为 true，表示有放回取样。
     * fraction：抽样比例，即从原始数据集中抽取的样本的比例，取值范围为 [0, 1]，默认为 0.1。
     * seed：随机数种子，用于初始化随机数生成器，从而实现每次抽样的结果一致性。
     */
    val result: Array[Int] = rdd.sample(false, 0.5).collect()

    println(result.mkString(","))
  }



}
