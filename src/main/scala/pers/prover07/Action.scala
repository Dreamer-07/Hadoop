package pers.prover07

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * 行动算子
 */
object Action {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("map")
    val sc = new SparkContext(conf)
    testTakeSample(sc)
    sc.stop()
  }

  def testForeachPartition(sc: SparkContext): Unit = {
    val rdd: RDD[Int] = sc.parallelize(List(1, 3, 5, 10, 29), 2)

    // 定义自定义处理函数
    def process(datas: Iterator[Int]): Unit = {
      println("开始读取一个分区")
      val result = ListBuffer[Int]()

      for (elem <- datas) {
        result.append(elem)
      }

      println("当前分区的数据是:", result)
    }

    // foreachPartition: 对每个分区进行 process 函数操作
    rdd.foreachPartition(process)
  }

  /**
   * 将数据保存到文件中(本地/hdfs)
   */
  def testSaveAsTextFile(sc: SparkContext): Unit = {
    val rdd: RDD[Int] = sc.parallelize(List(1, 3, 5, 10, 29), 2)

    rdd.saveAsTextFile("hdfs://hadoop01:9000/spark/output/file")
  }

  /**
   * 根据某个key进行数量统计(适合 key value 型数据)
   * @param sc
   */
  def testCountBy(sc: SparkContext): Unit = {
    val rdd: RDD[(String, Int)] = sc.parallelize(List(("a", 1), ("b", 1), ("a", 2), ("c", 1)))

    val result: collection.Map[String, Long] = rdd.countByKey()

    result.foreach(println)
  }

  /**
   * 按照传入的计算逻辑进行聚合操作
   * @param sc
   */
  def testReduce(sc: SparkContext): Unit = {
    val rdd: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5))

    val res: Int = rdd.reduce(_ + _)

    println(res)
  }

  /**
   * 和 reduce 相似，主要在 分区内/间 进行聚合
   * @param sc
   */
  def testFlod(sc: SparkContext): Unit = {
    val rdd: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 3)

    /**
     * 第一个参数为初始值
     * 第二个参数为分区内/间的聚合操作
     */
    val result: Int = rdd.fold(10)(_ + _)

    println(result)
  }

  def testFirstTakeCount(sc: SparkContext): Unit = {
    val rdd: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 3)

    // first：获取第一个
    val res1: Int = rdd.first()
    println("first:", res1)

    // take(n): 获取前几个元素组成的数组
    val res2: Array[Int] = rdd.take(8)
    println(res2.mkString(","))

    // count(): 计数
    val res3: Long = rdd.count()
    println("count:", res3)
  }

  def testTopTakeOrdered(sc: SparkContext): Unit = {
    val rdd: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 3)

    // top(n): 取出RDD降序排序后的topN
    val res1: Array[Int] = rdd.top(3)
    println(res1.mkString(","))

    // top(n)(Ordering.Int.reverse)取出RDD升序后的topN
    val res2: Array[Int] = rdd.top(3)(Ordering.Int.reverse)
    println(res2.mkString(","))

    // takeOrded 取出RDD升序排序后的topN
    val res3: Array[Int] = rdd.takeOrdered(3)
    println(res3.mkString(","))

    // takeOrdered(n)(Ordering.Int.reverse)取出RDD降序排序后的topN
    val res4: Array[Int] = rdd.takeOrdered(3)(Ordering.Int.reverse)
    println(res4.mkString(","))
  }

  def testTakeSample(sc: SparkContext): Unit = {
    /*
    withReplacement：true可以重复，false:不能重复。数据位置是否重复不是值
    num：抽样的元素个数
    seed:随机种子，数值型的；RDD元素相同的情况下，相同的种子抽样出的元素也相同
    def takeSample(
       withReplacement: Boolean,
       num: Int,
       seed: Long = Utils.random.nextLong): Array[T]
    * */

    val rdd: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 3)
    val res: Array[Int] = rdd.takeSample(withReplacement = true, 4)
    println(res.mkString(","))

  }




}
