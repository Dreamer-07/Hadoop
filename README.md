# Hadoop

## 入门

### 相关概念

大数据：

- 大数据是一门概念，也是一门技术，是以Hadoop为代表的大数据平台框架上进行各种数据分析的技术。

- 大数据包括了以 Hadoop 和 Spark 为代表的基础大数据框架，还包括**实时数据处理，离线数据处理，数据分析，数据挖掘和用机器算法**进行预测分析等技术。

Hadoop：

- 分布式计算的解决方案
- 主要解决大数据的两个核心问题：
  - 数据存储问题 - HDFS 分布式文件系统
  - 分布式计算问题 - MapReduce
- 特点：
  - 优势：
    - 支持超大文件。HDFS存储的文件可以支持TB和PB级别的数据。
    - 检测和快速应对硬件故障。数据备份机制，NameNode 通过**心跳机制**来检测 DataNode是否还存在。
    - 高扩展性。可建构在廉价机上，实现线性（横向）扩展，当集群增加新节点之后，**NameNode 也可以感知，将数据分发和备份到相应的节点上**。
    - **生态体系**
  - 缺点：
    - 不能做到低延迟。高数据吞吐量做了优化，牺牲了获取数据的延迟。(CAP)
    - 不适合大量的小文件存储。
    - 文件修改效率低。HDFS适合一次写入，多次读取的场景。
- 组成：
  - HDFS：用于通过流访问模式，用普通、廉价硬件集群来存储大数据
  - MapReduce：为了处理存储在 HDFS 中的数据
  - YRAN：用于管理集群资源，同时也是 Hadoop 中协调应用程序运行时的作业调度框架。

### HDFS

#### 主从架构

- Master + Salve 的主从架构，主要由  **Name-Node** + **Secondary NameNode** + **DataNode** 组成

  ![image-20230328092653435](https://cdn.jsdelivr.net/gh/Dreamer-07/pic-go@main/202303280928318.png)

  

- **NameNode**：

  - Master 节点，管理 HDFS 的名称空间和数据块映射信存储元数据与文件到数据块映射的地方。
  - 同时 Hadoop 提供了高可用集群的配置，集群中有两个 NameNode 节点，一台 active 主节点，另一台 standby 备用节点，两者数据时刻保持一致。当主节点不可用时，备用节点马上自动切换，用户感知不到，避免了NameNode的单点问题。

- **Secondary NameNode**：

  - 辅助 NameNode，分担 NameNode工作，紧急情况下可辅助恢复NameNode。

- **DataNode**：

  - Slave 节点，负责数据块的存储，执行数据库的读写并汇报存储信息给 NameNode

#### 文件读写

- 文件是按照数据块的方式存储到 DataNode 上的，数据块是抽象块，作为存储和传输单元，而并非整个文件(最大为 128M)

- 为了保证数据的安全，数据块都会带备份，提高数据的容错能力和可用性

- HDFS 读取文件流程

  ![图片](https://cdn.jsdelivr.net/gh/Dreamer-07/pic-go@main/202303280930606.png)

  1. client 访问 Namenode，查询元数据信息，获取文件的数据块位置列表，返回输入流对象
  2. 根据就近原则选择一台 datanode 服务器，请求建立输入流
  3. DataNode 向输入流中写数据，以 packet 为单位来校验
  4. 关闭输入流

- HDFS 写文件流程

  ![图片](https://cdn.jsdelivr.net/gh/Dreamer-07/pic-go@main/202303280930140.png)

  1. 向 NameNode 通信请求上传文件，NameNode检查目标文件是否已存在，父目录是否存在。
  2. NameNode返回确认可以上传。
  3. client 对文件进行切分，切完后会请求 NameNode 得到要上传的 DataNode 服务器
  4. 建立 Block 传输管道
  5. client请求一台DataNode上传数据，第一个DataNode收到请求会继续调用第二个DataNode，然后第二个调用第三个DataNode，将整个通道建立完成，逐级返回客户端。
  6. client开始往A上传第一个block，当然在写入的时候DataNode会进行数据校验，第一台DataNode收到后就会传给第二台，第二台传给第三台。
  7. 当一个block传输完成之后，client再次请求NameNode上传第二个block的服务器。

### MapReduce

- 一种编程模型，是**抽象的理论**，采用了分而治之的思想。整体分为 Map + Reduce
  - Map: 每个文件分片由单独的机器去处理
  - Reduce：将各个机器的计算结果汇总并得到最终的结果
- 工作流程：提交任务 -> 拆分成 Map 任务 -> 分配到不同节点上执行 -> 每一个任务都会产生一些中间文件 -> 使用中间文件作为 Reduce 任务的输入数据 -> Reduce 将若干个 Map 的输出总的一起输出

### YARN

- Hadoop 的资源管理框架，主要由 **ResourceManager** + **ApplicationMaster** + **NodeManager** 组成

  - ResourceManager：整个系统只有一个，负责资源的调度，包含 **定时调用器(Scheduler) + 应用管理器(ApplicationManager)**

    - Scheduler：当客户端提交任务时，根据所需要的资源以及当前集群的资源状况进行分配。(只分配资源，不做监控以及应用程序的状态跟踪)
    - ApplicationManager：监控以及应用程序的状态跟踪

  - ApplicationMaster：每当 Client 提交一个 Application 时候，就会新建一个 ApplicationMaster 。由这个 ApplicationMaster 去与 ResourceManager 申请容器资源，获得资源后会将要运行的程序发送到容器上启动，然后进行分布式计算。

    > 这里可能有些难以理解，为什么是把运行程序发送到容器上去运行？如果以传统的思路来看，是程序运行着不动，然后数据进进出出不停流转。但当数据量大的时候就没法这么玩了，因为海量数据移动成本太大，时间太长。但是中国有一句老话**山不过来，我就过去。**大数据分布式计算就是这种思想，既然大数据难以移动，那我就把容易移动的应用程序发布到各个节点进行计算呗，这就是大数据分布式计算的思路。

  - NodeManager: ResourceManager 在每台机器的上代理，负责容器的管理，并监控他们的资源使用情况（cpu，内存，磁盘及网络等），以及向 ResourceManager/Scheduler 提供这些资源使用报告。

  ![img](https://cdn.jsdelivr.net/gh/Dreamer-07/pic-go@main/202303280930471.jpeg)

  > Container：Yarn 对资源做的一层抽象。

### 安装

[hadoop安装 + 历史服务器 + 日志收集](https://blog.csdn.net/yy8623977/article/details/124408440)



### 常用脚本

集群分发脚本(可以用这个同步配置文件的修改)

```sh
if [ $# -lt 1 ]
then
    echo Not Enough Arguement!
    exit;
fi
#2. 遍历集群所有机器
for host in hadoop01 hadoop02
do
         echo ==================== $host ====================
         #3. 遍历所有目录，挨个发送
         for file in $@
         do
                        #4. 判断文件是否存在
                   if [ -e $file ]
                  then
                                #5. 获取父目录
                    pdir=$(cd -P $(dirname $file); pwd)
                                        #6. 获取当前文件的名称
                                        fname=$(basename $file)
                                        ssh $host "mkdir -p $pdir" #-p保证哪怕已经存在也不报错
                                        rsync -av $pdir/$fname $host:$pdir
              else
                    echo $file does not exists!
            fi
         done
done
```

启动 hadoop 集群(记得改成对应的主机名)

```sh
#!/bin/bash

if [ $# -lt 1 ]
then
    echo "No Args Input..."
    exit ;
fi

case $1 in
"start")
        echo " =================== 启动 hadoop集群 ==================="

        echo " --------------- 启动 hdfs ---------------"
        ssh hadoop01 "/opt/module/hadoop-3.3.1/sbin/start-dfs.sh"
        echo " --------------- 启动 yarn ---------------"
        ssh hadoop01 "/opt/module/hadoop-3.3.1/sbin/start-yarn.sh"
        echo " --------------- 启动 historyserver ---------------"
        ssh hadoop01 "/opt/module/hadoop-3.3.1/bin/mapred --daemon start historyserver"
;;
"stop")
        echo " =================== 关闭 hadoop集群 ==================="

        echo " --------------- 关闭 historyserver ---------------"
        ssh hadoop01 "/opt/module/hadoop-3.3.1/bin/mapred --daemon stop historyserver"
        echo " --------------- 关闭 yarn ---------------"
        ssh hadoop01 "/opt/module/hadoop-3.3.1/sbin/stop-yarn.sh"
        echo " --------------- 关闭 hdfs ---------------"
        ssh hadoop01 "/opt/module/hadoop-3.3.1/sbin/stop-dfs.sh"
;;
*)
    echo "Input Args Error..."
;;
esac
```

查看集群节点的 jps(记得改成对应的主机名)

```sh
#!/bin/bash

for host in hadoop01 hadoop02
do
        echo =============== $host ===============
        ssh $host jps
done
```

## HDFS

### 概述

- 一种分布式文件管理系统
- HDFS 适合一次写入，多次读出的场景。一个文件经过创建、写入和关闭之后就不需要改变。
- 优缺点
  - 优点
    - 高容错性
    - 适合处理大数据
    - 可以构建在廉价机器上，通过多副本机制，提高可靠性
  - 缺点
    - 不适合低延时访问
    - 无法高效的对大量小文件进行存储
    - 不支持并发写入，文件随机修改(只支持数据append)

### Shell 操作

> hadoop fs 和 hdfs dfs 这两个是一个东西

- 命令大全

  ![image-20230328111444783](https://cdn.jsdelivr.net/gh/Dreamer-07/pic-go@main/202303281114869.png)

#### 常用命令

- `hdfs dfs -help 命令`: 查看 hdfs 相关命令的使用方法
- `hdfs dfs -mkdir 文件夹名 `： 创建文件夹
- `-moveFromLocal 本地文件地址 hdfs文件地址`: 从本地**剪切**复制粘贴到 HDFS
- `-copyFromLocal 本地文件地址 hdfs文件地址`: 从本地文件系统拷贝到 HDFS
- `-put  本地文件地址 hdfs文件地址`：等同于 `copyFromLocal ` ，生产环境更习惯用 put
- `-appendToFile 本地文件地址 hdfs文件地址`：追加一个文件内容到已经存在的文件末尾
- `-copyToLocal hdfs文件地址 本地文件地址`: 从 HDFS 拷贝到本地
- `-get hdfs文件地址 本地文件地址`: 等同于 `copyToLocal `，生产环境更习惯用 get
- `-ls 目录`：显示目录信息
- `-cat 文件`：显示文件内容
- `-chgrp / -chomod / -chown`: 和 Linux 系统用法一样，修改文件所属权限
- `-cp`: 从 hdfs 的路径拷贝到 hdfs 的另一个路径
- `-mv`: 从 hdfs 中移动文件
- `-tail hdfs文件地址`: 显示文件末尾 1kb 的数据
- `-rm`: 删除文件/文件夹
- `-rm -r`: 递归删除目录及其内容
- `-du`: 统计文件夹大小及内容信息
- `-setrep 副本数量 hdfs文件` : 设置 HDFS 中文件的副本数量

### 客户端API操作

依赖文件

```xml
<!-- hadoop依赖 -->
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-client</artifactId>
    <version>3.1.3</version>
</dependency>
<!-- Junit测试依赖 -->
<dependency>
    <groupId>junit</groupId>
    <artifactId>junit</artifactId>
    <version>4.12</version>
</dependency>
<!-- log4j日志记录依赖 -->
<dependency>
    <groupId>org.slf4j</groupId>
    <artifactId>slf4j-log4j12</artifactId>
    <version>1.7.30</version>
</dependency>
```

日志格式文件

```properties
log4j.rootLogger=INFO, stdout 
log4j.appender.stdout=org.apache.log4j.ConsoleAppender 
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout 
log4j.appender.stdout.layout.ConversionPattern=%d %p [%c] - %m%n 
log4j.appender.logfile=org.apache.log4j.FileAppender 
log4j.appender.logfile.File=target/spring.log 
log4j.appender.logfile.layout=org.apache.log4j.PatternLayout 
log4j.appender.logfile.layout.ConversionPattern=%d %p [%c] - %m%n
```

客户端API使用

```java
public class HdfsClientTest {

    private FileSystem fileSystem;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        // 创建一个配置文件
        Configuration configuration = new Configuration();
        // 返回主机名而不是ip地址
        configuration.set("dfs.client.use.datanode.hostname", "true");

        // 链接
        fileSystem = FileSystem.get(new URI("hdfs://hadoop01:9000"), configuration, "root");
    }

    // 关闭链接
    @After
    public void close() throws IOException {
        fileSystem.close();
    }

    @Test
    public void testMkdir() throws IOException {
        fileSystem.mkdirs(new Path("/java"));
    }

    /**
     * 上传
     *
     * @throws IOException
     */
    @Test
    public void testCopyFromLocalFile() throws IOException {
        // 是否删除源数据，是否覆盖，本地文件地址，hdfs文件地址
        fileSystem.copyFromLocalFile(false, true, new Path("D:/StudyCode/BigData/Hadoop/README.md"), new Path("/java"));
    }

    /**
     * 下载
     *
     * @throws IOException
     */
    @Test
    public void testCopyToLocalFile() throws IOException {
        // 是否删除源数据，hdfs文件地址，本地文件地址，是否开启文件校验
        fileSystem.copyToLocalFile(false, new Path("/java/README.md"), new Path("D:\\"), true);
    }

    /**
     * 文件更名和移动
     *
     * @throws IOException
     */
    @Test
    public void testMv() throws IOException {
        fileSystem.rename(new Path("/java/README.md"), new Path("/java/test.md"));
        // 也可以对目录进行更名
    }

    /**
     * 查看文件详情
     */
    @Test
    public void testDetail() throws IOException {
        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/"), true);

        // 遍历文件
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println("========" + fileStatus.getPath() + "=========");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());

            // 获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }

    }


    /**
     * 文件和文件夹判断
     * @throws IOException
     */
    @Test
    public void testFileType() throws IOException {
        FileStatus[] listStatus = fileSystem.listStatus(new Path("/"));

        for (FileStatus fileStatus : listStatus) {
            // 如果是文件
            if (fileStatus.isFile()) {
                System.out.println("f:" + fileStatus.getPath().getName());
            } else {
                System.out.println("d:" + fileStatus.getPath().getName());
            }
        }
    }

}
```

## MapReduce

### 概述

- MapReduce 是一个**分布式运算程序**的编程框架，是用户开发基于 Hadoop 的数据分析应用的核心框架。
- 核心功能：将**用户编写的业务逻辑代码**和**自带默认组件**组合成一个完整的**分布式运算程序**
- 优缺点：
  - 优点：
    - 易于编程，实现接口后用户只关心业务逻辑
    - 良好扩展性，可以动态增加服务器，解决计算资源不够的问题
    - 高容错性，任何一台机器挂掉，可以将任务转移到其他节点
    - 适合海量数据计算，可以实现几千台服务器共同计算
  - 缺点：
    - 不擅长实时计算
    - 不擅长流式计算
    - 不擅长DAG有向无环图计算

### 常用数据序列化类型

![img](https://cdn.jsdelivr.net/gh/Dreamer-07/pic-go@main/202303281637722.png)

> 除了 `Text` 类型的其他类型都是在Java类型的后面加了 `Writable`

### 编程规范

> Mapper + Reducer  + Driver

- Mapper：
  1. 用户自定义的Mapper要继承对应的父类
  2. Mapper的输入数据是KV对（K：偏移量，V：对应的内容）的形式(KV的类型可自定义) 
  3. Mapper中的业务逻辑写在 **map()** 方法中
  4. Mapper的输出数据是 KV对的形式(KV的类型可自定义) 
  5. **map()** 方法(MapTask进程）会对每一个数据<K,V>调用一次
- Reducer：
  1. 用户自定义的Reducer要继承对应的父类
  2. Reducer 的输入数据类型对应 Mapper 的输出数据类型，也是KV
  3. Reducer的业务逻辑写在reduce()方法中
  4. ReduceTask 进程对每一组相同k的<k,v>组调用一次reduce()方法
- Driver：相当于YARN集群的客户端，用于提交我们整个程序到YARN集群，提交的是封装了MapReduce程序相关运行参数的job对象

### WordCount 实操

定义 Mapper：

```java
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text reduceKey = new Text();

    private IntWritable reduceValue = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        // 获取一行数据
        String str = value.toString();
        // 切割数据
        String[] strArr = str.split(" ");

        for (String s : strArr) {
            // 设置 key 值
            reduceKey.set(s);
            // 写出
            context.write(reduceKey, reduceValue);
        }
    }
}
```

定义 Reduce：

```java
public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     *
     * @param key           map 输出的 key
     * @param values        map 相同 key 对应的 values
     * @param context       reduce 上下文环境
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        // 对 value / key 进行 reduce 操作
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        IntWritable writable = new IntWritable(sum);

        context.write(key, writable);
    }
}
```

定义 Driver：构建提交给 YARN 的 job 程序

```java
public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    Configuration configuration = new Configuration();
    // 获取 job
    Job job = Job.getInstance(configuration);

    // 设置 jar 包路径
    job.setJarByClass(WordCountDriver.class);

    // 关联 mapper 和 reduce
    job.setMapperClass(WordCountMapper.class);
    job.setReducerClass(WordCountReduce.class);

    // 设置 map 输出的 kv 类型
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    // 设置最终输出的 kv 类型
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    // 设置输入路径和输出路径
    FileInputFormat.setInputPaths(job, new Path(WordCountDriver.class.getClassLoader().getResource("text.txt").getPath()));
    FileOutputFormat.setOutputPath(job, new Path("D:\\StudyCode\\BigData\\Hadoop\\src\\main\\resources\\output"));

    // 提交 job
    boolean wait = job.waitForCompletion(true);

    System.exit(wait ? 0 : 1);
}
```

上述的 Driver 只能在本地 windows 上运行，如果需要在集群上运行需要修改成下述：

```java
// 获取参数
if (args.length != 2) {
    System.exit(100);
}

...

// 设置输入路径和输出路径
// 服务器集群
FileInputFormat.setInputPaths(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
```

使用 maven package 打包后上传到服务器上输入：

```bash
hadoop jar [jar包] Driver主类全类名 hdfs上的输入文件路径 hdfs上的输出文件路径
```



## 进阶

### MapReduce

### HDFS

### YARN
